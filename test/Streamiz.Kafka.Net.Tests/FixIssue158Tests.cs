using System;
using Google.Protobuf.Reflection;
using Microsoft.VisualBasic;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests
{
    public class FixIssue158Tests
    {
        public class Person
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public string LocationId { get; set; }
            public string JobId { get; set; }
        }

        public class Location
        {
            public string City { get; set; }
            public string ZipCode { get; set; }
        }
        
        public class Job
        {
            public string Title { get; set; }
            public float Salary { get; set; }
        }

        public class PersonLocation
        {
            public Person Person { get; set; }
            public Location Location { get; set; }
        }

        public class PersonJobLocation
        {
            public Location Location { get; set; }
            public Person Person { get; set; }
            public Job Job { get; set; }
        }
        
        
        [Test]
        public void FixIssue158()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-issue-158";

            var builder = new StreamBuilder();

            var stringSerdes = new StringSerDes();
            var personSerdes = new JsonSerDes<Person>();
            var locationSerdes = new JsonSerDes<Location>();
            var jobSerdes = new JsonSerDes<Job>();
            
            var personStream = builder.Stream("persons", stringSerdes, personSerdes);
            var locationStream = builder.Stream("locations", stringSerdes, locationSerdes);
            var jobStream = builder.Stream("jobs", stringSerdes, jobSerdes);

            var locationTable = locationStream.ToTable(
                InMemory.As<string, Location, StringSerDes, JsonSerDes<Location>>());
            
            var jobTable = jobStream.ToTable(
                InMemory.As<string, Job, StringSerDes, JsonSerDes<Job>>());
            
            personStream
                .SelectKey((_, v) => v.LocationId)
                .Join<Location, PersonLocation, JsonSerDes<Location>, JsonSerDes<PersonLocation>>(locationTable,
                    (person, location) => new PersonLocation {
                        Person = person,
                        Location = location
                    })
                .SelectKey((_, v) => v.Person.JobId)
                .Join<Job, PersonJobLocation, JsonSerDes<Job>, JsonSerDes<PersonJobLocation>>(jobTable,
                    (personLocation, job) => new PersonJobLocation {
                        Person = personLocation.Person,
                        Location = personLocation.Location,
                        Job = job
                    })
                .To<StringSerDes, JsonSerDes<PersonJobLocation>>("person-job-location");
            
            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var personInput = driver.CreateInputTopic("persons", stringSerdes, personSerdes);
                var locationInput = driver.CreateInputTopic("locations", stringSerdes, locationSerdes);
                var jobInput = driver.CreateInputTopic("jobs", stringSerdes, jobSerdes);

                var output = driver.CreateOuputTopic("person-job-location",
                    TimeSpan.FromSeconds(10),
                    stringSerdes,
                    new JsonSerDes<PersonJobLocation>());
                
                jobInput.PipeInput("job1", new Job(){Salary = 100000F, Title = "Engineer"});
                locationInput.PipeInput("loc1", new Location() {City = "Paris", ZipCode = "75004"});
                personInput.PipeInput("person1", new Person(){Age = 24, Name = "Thomas", JobId = "job1", LocationId = "loc1"});

                var record = output.ReadKeyValue();
                Assert.IsNotNull(record);
            }
        }
    }
}