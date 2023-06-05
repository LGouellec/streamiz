using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests
{
    public class FixIssue221Tests
    {
        public class Person
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public int LocationId { get; set; }
            public string JobId { get; set; }
        }

        public class Location
        {
            public string City { get; set; }
            public string ZipCode { get; set; }
        }

        public class PersonLocation
        {
            public Person Person { get; set; }
            public Location Location { get; set; }
        }

        [Test]
        public void FixIssue221()
        {
            BuildTopology();
        }
   
        [Test]
        public void FixIssue221LeftJoin()
        {
            BuildTopology(true);
        }


        private void BuildTopology(bool leftJoin = false)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-issue-221";

            var builder = new StreamBuilder();

            var stringSerdes = new StringSerDes();
            var intSerdes = new Int32SerDes();
            var personSerdes = new JsonSerDes<Person>();
            var locationSerdes = new JsonSerDes<Location>();
            
            var personStream = builder.Stream("person", stringSerdes, personSerdes);
            var locationTable = builder.Table(
                "location",
                intSerdes,
                locationSerdes,
                InMemory.As<Int32, Location>("location-store"));

            var stream = personStream
                .Map((_, v) => KeyValuePair.Create(v.LocationId, v));

            IKStream<Int32, PersonLocation> personLocationStream;
            
            if (leftJoin)
                personLocationStream = stream.LeftJoin(locationTable,
                    ((person, location) => new PersonLocation {Location = location, Person = person}),
                    new StreamTableJoinProps<int, Person, Location>(intSerdes, personSerdes, locationSerdes));
            else
                personLocationStream = stream.Join(locationTable,
                        ((person, location) => new PersonLocation {Location = location, Person = person}),
                        new StreamTableJoinProps<int, Person, Location>(intSerdes, personSerdes, locationSerdes));

            personLocationStream.To<Int32SerDes, JsonSerDes<PersonLocation>>("person-location");
            
            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var personInput = driver.CreateInputTopic("person", stringSerdes, personSerdes);
                var locationInput = driver.CreateInputTopic("location", intSerdes, locationSerdes);

                var output = driver.CreateOuputTopic("person-location",
                    TimeSpan.FromSeconds(10),
                    intSerdes,
                    new JsonSerDes<PersonLocation>());
                
                locationInput.PipeInput(1, new Location() {City = "Paris", ZipCode = "75004"});
                personInput.PipeInput("person1", new Person(){Age = 24, Name = "Thomas", JobId = "job1", LocationId = 1});

                var record = output.ReadKeyValue();
                Assert.IsNotNull(record);
            }
        }
    }
}