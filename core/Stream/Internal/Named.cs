using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class Named
    {
        private static int MAX_NAME_LENGTH = 249;
        private readonly string name;

        public Named(string name)
        {
            this.name = name;
        }

        private void Validate(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new TopologyException("Name is illegal, it can't be empty");
            if (name.Equals(".") || name.Equals(".."))
                throw new TopologyException("Name cannot be \".\" or \"..\"");
            if (name.Length > MAX_NAME_LENGTH)
                throw new TopologyException($"Name is illegal, it can't be longer than {MAX_NAME_LENGTH} characters, name: {name}");
            if (!ContainsValidPattern(name))
                throw new TopologyException($"Name \"{name}\" is illegal, it contains a character other than ASCII alphanumerics, '.', '_' and '-'");
        }

        private static bool ContainsValidPattern(string topic)
        {
            for (int i = 0; i < topic.Length; ++i)
            {
                char c = topic[i];

                // We don't use Character.isLetterOrDigit(c) because it's slower
                var validLetterOrDigit = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z');
                var validChar = validLetterOrDigit || c == '.' || c == '_' || c == '-';
                if (!validChar)
                {
                    return false;
                }
            }
            return true;
        }

        internal string SuffixWithOrElseGet(string suffix, string other)
        {
            if (!string.IsNullOrEmpty(name))
            {
                return $"{name}{suffix}";
            }
            else
            {
                return other;
            }
        }

        internal string SuffixWithOrElseGet(string suffix, INameProvider provider, string prefix)
        {
            // We actually do not need to generate processor names for operation if a name is specified.
            // But before returning, we still need to burn index for the operation to keep topology backward compatibility.
            if (!string.IsNullOrEmpty(name))
            {
                provider.NewProcessorName(prefix);

                string suffixed = $"{name}{suffix}";
                // Re-validate generated name as suffixed string could be too large.
                Validate(suffixed);

                return suffixed;
            }
            else
            {
                return provider.NewProcessorName(prefix);
            }
        }

        internal string OrElseGenerateWithPrefix( INameProvider provider,  string prefix)
        {
            // We actually do not need to generate processor names for operation if a name is specified.
            // But before returning, we still need to burn index for the operation to keep topology backward compatibility.
            if (!string.IsNullOrEmpty(name))
            {
                provider.NewProcessorName(prefix);
                return name;
            }
            else
            {
                return provider.NewProcessorName(prefix);
            }
        }
    }
}
