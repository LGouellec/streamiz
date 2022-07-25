using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// This class saves out a map of topic/partition=&gt;offsets to a file. The format of the file is UTF-8 text containing the following:
    /// <para>
    /// &lt;version&gt;
    /// &lt;n&gt;
    /// &lt;topic_name_1&gt; &lt;partition_1&gt; &lt;offset_1&gt;
    /// .
    /// .
    /// .
    /// &lt;topic_name_n&gt; &lt;partition_n&gt; &lt;offset_n&gt;
    /// </para>
    /// The first line contains a number designating the format version (currently 0), the get line contains
    /// a number giving the total number of offsets. Each successive line gives a topic/partition/offset triple
    /// separated by spaces.
    /// </summary>
    public class OffsetCheckpointFile : IOffsetCheckpointManager
    {
        private static readonly int VERSION = 0;

        
        /// <summary>
        /// Use a negative sentinel when we don't know the offset instead of skipping it to distinguish it from dirty state
        /// and use -4 as the -1 sentinel may be taken by some producer errors and -2 in the
        /// subscription means that the state is used by an active task and hence caught-up and
        /// -3 is also used in the subscription.
        /// </summary>
        public static readonly long OFFSET_UNKNOWN = -4L;

        private readonly string path;
        private readonly object _lock = new object();
        private readonly ILogger logger = Logger.GetLogger(typeof(OffsetCheckpointFile));
        
        /// <summary>
        /// Checkpoint file name (can't be override).
        /// </summary>
        public static readonly string CHECKPOINT_FILE_NAME = ".checkpoint";

        /// <summary>
        /// Constructor with base folder's checkpoint file
        /// </summary>
        /// <param name="path">parent folder on checkpoint file</param>
        public OffsetCheckpointFile(string path) => this.path = Path.Combine(path, CHECKPOINT_FILE_NAME);

        /// <summary>
        /// Delete the parent folder.
        /// </summary>
        /// <param name="taskId">current task</param>
        public void Destroy(TaskId taskId)
        {
            if (File.Exists(path))
                File.Delete(path);
        }

        /// <summary>
        /// Read the offsets from the local checkpoint file, skipping any negative offsets it finds.
        /// </summary>
        /// <param name="taskId">current task</param>
        /// <returns>dictionary {TopicPartition, long}</returns>
        /// <exception cref="IOException">throws if an error appeared during reading process file</exception>
        /// <exception cref="ArgumentException">throws if version in checkpoint file is incorrect</exception>
        public IDictionary<TopicPartition, long> Read(TaskId taskId)
        {
            lock (_lock)
            {
                try
                {
                    string checkpointFilePath = path;
                    if (!File.Exists(path) && File.Exists(RecoveryFileName(path)))
                    {
                        checkpointFilePath = RecoveryFileName(path);
                        logger.LogWarning($"Read checkpoint offsets from recovery file : {checkpointFilePath}");
                    }

                    using (StreamReader fileReader = new StreamReader(checkpointFilePath))
                    {
                        int version = ReadInt(fileReader);
                        switch (version)
                        {
                            case 0:
                                int numberLines = ReadInt(fileReader);
                                var offsets = new Dictionary<TopicPartition, long>();
                                String line = fileReader.ReadLine();
                                while (line != null)
                                {
                                    String[] pieces = line.Split(' ');
                                    if (pieces.Length != 3)
                                    {
                                        throw new IOException($"Malformed line in offset checkpoint file: {line}.");
                                    }

                                    String topic = pieces[0];
                                    int partition = Int32.Parse(pieces[1]);
                                    TopicPartition tp = new TopicPartition(topic, partition);
                                    long offset = Int64.Parse(pieces[2]);
                                    if (IsValid(offset))
                                    {
                                        offsets.Add(tp, offset);
                                    }
                                    else
                                    {
                                        logger.LogWarning($"Read offset={offset} from checkpoint file for {tp}");
                                        --numberLines;
                                    }

                                    line = fileReader.ReadLine();
                                }
                                if (offsets.Count != numberLines)
                                {
                                    throw new IOException($"Expected {numberLines} entries but found only {offsets.Count}");
                                }
                                return offsets;

                            default:
                                throw new ArgumentException($"Unknown offset checkpoint version: {version}");

                        }
                    }
                }
                catch (IOException)
                {
                    return new Dictionary<TopicPartition, long>();
                }
            }
        }

        /// <summary>
        /// Write the given offsets to the checkpoint file. All offsets should be non-negative.
        /// </summary>
        /// <param name="taskId">current task</param>
        /// <param name="data">topic/partition offsets</param>
        /// <exception cref="StreamsException">throws if an offset is invalid</exception>
        public void Write(TaskId taskId, IDictionary<TopicPartition, long> data)
        {
            if (!data.Any())
            {
                Destroy(taskId);
                return;
            }

            lock (_lock)
            {
                string tmpFile = string.Concat(path, ".tmp");
                using (var fileStream = File.Create(tmpFile))
                using (var writerStream = new StreamWriter(fileStream))
                {
                    WriteInt(writerStream, VERSION);
                    WriteInt(writerStream, data.Count);
                    foreach(KeyValuePair<TopicPartition, long> kv in data){
                        if(IsValid(kv.Value))
                            WriteEntry(writerStream, kv.Key, kv.Value);
                        else{
                            logger.LogError($"Received offset {kv.Value} to write to checkpoint file for topic:{kv.Key.Topic}|partition:{kv.Key.Partition}");
                            throw new StreamsException("Unable to write a negative offset to the checkpoint file");
                        }
                    }
                    writerStream.Flush();
                    fileStream.Flush();
                }

                if (File.Exists(path))
                    File.Replace(tmpFile, path, RecoveryFileName(path));
                else
                    File.Move(tmpFile, path);
            }
        }

        /// <summary>
        /// Not used in this implementation
        /// </summary>
        /// <param name="config"></param>
        /// <param name="taskId"></param>
        public void Configure(IStreamConfig config, TaskId taskId)
        {
            // Nothing here
        }

        #region Private

        private string RecoveryFileName(string path) => string.Concat(path, ".rec");

        private void WriteEntry(StreamWriter fileStream, TopicPartition key, long value)
        {
            fileStream.WriteLine($"{key.Topic} {key.Partition.Value} {value}");
        }

        private bool IsValid(long offset)
        {
            return offset >= 0L || offset == OFFSET_UNKNOWN;
        }

        private int ReadInt(StreamReader fileReader)
        {
            string line = fileReader.ReadLine();

            if (line == null)
                throw new EndOfStreamException("File ended prematurely");

            return Int32.Parse(line);
        }

        private void WriteInt(StreamWriter fileWriter, int number)
        {
            fileWriter.WriteLine(number.ToString());
        }

        #endregion
    }
}
