using System;
using System.Threading;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests
{
    public class Test
    {
        class PageView
        {
            public string user { get; set; }
            public string url { get; set; }
        }

        class UserProfile
        {
            public string region { get; set; }
        }

        class ViewRegion
        {
            public string page { get; set; }
            public string user { get; set; }
            public string region { get; set; }
        }


        [Test]
        public void tt()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "page-views-example";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.CommitIntervalMs = 3000;

            var builder = new StreamBuilder();
            string PAGE_VIEW_TOPIC = "page-views",
                USER_PROFILE_TOPIC = "users",
                PAGE_VIEW_REGION_TOPIC = "page-views-enriched";

// streams the page views and re-key per user
            var viewsByUser = builder.Stream<string, PageView, StringSerDes, JsonSerDes<PageView>>(PAGE_VIEW_TOPIC)
                .SelectKey((k, v) => v.user);

// materialize a table from the users
            var userRegions = builder
                .Stream<string, UserProfile, StringSerDes, JsonSerDes<UserProfile>>(USER_PROFILE_TOPIC)
                .MapValues((v) => v.region)
                .ToTable(
                    InMemory.As<string, string>($"{USER_PROFILE_TOPIC}-store")
                        .WithValueSerdes<StringSerDes>()
                        .WithKeySerdes<StringSerDes>());

// join the page views event with the user referential
            viewsByUser.Join(userRegions,
                    (view, region) => new ViewRegion
                    {
                        page = view.url,
                        user = view.user,
                        region = region
                    })
                .To<StringSerDes, JsonSerDes<ViewRegion>>(PAGE_VIEW_REGION_TOPIC);

            var streamiz = new KafkaStream(builder.Build(), config);
            streamiz.Start();
            Thread.Sleep(60000);
            streamiz.Dispose();
        }
    }
}