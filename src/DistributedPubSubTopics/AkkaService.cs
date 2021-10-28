using System.IO;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Bootstrap.Docker;
using Akka.Configuration;
using Petabridge.Cmd.Cluster;
using Petabridge.Cmd.Host;
using Petabridge.Cmd.Remote;
using Microsoft.Extensions.Hosting;
using System.Threading;
using System;
using Akka.Cluster.Tools.PublishSubscribe;
using Akka.DependencyInjection;
using Akka.Event;

namespace DistributedPubSubTopics
{
    public sealed class DistributedPubSubTester : ReceiveActor
    {
        private readonly IActorRef _mediator;
        private readonly ILoggingAdapter _logging = Context.GetLogger();
        
        public DistributedPubSubTester()
        {
            _mediator = DistributedPubSub.Get(Context.System).Mediator;

            ReceiveAsync<SubscribeAck>(async s =>
            {
                _logging.Info("Received SubscribeAck for {0}", s.Subscribe.Topic);
                _mediator.Tell(new Publish("bad-topic", "IAmBad"));
                _logging.Info("Published to non-existent topic {0}", "bad-topic");
                await PollTopics();
                _mediator.Tell(new Unsubscribe("good-topic", Self));
            });
            
            ReceiveAsync<UnsubscribeAck>(async u =>
            {
                _logging.Info("Received UnsubscribeAck for {0}", u.Unsubscribe.Topic);
                _logging.Info("Polling remaining topics");
                await PollTopics();
                _mediator.Tell(new Publish("good-topic", "IAmAlsoBad"));
                _mediator.Tell(new Subscribe("good-topic", Self));
            });

            Receive<string>(s => _logging.Info(s));
        }

        private async Task PollTopics()
        {
            var topics = await _mediator.Ask<CurrentTopics>(GetTopics.Instance, TimeSpan.FromSeconds(3));
            foreach (var t in topics.Topics)
            {
                _logging.Info("Available topic: {0}", t);
            }
        }

        protected override void PreStart()
        {
            _mediator.Tell(new Subscribe("good-topic", Self));
        }
    }
    
    /// <summary>
    /// <see cref="IHostedService"/> that runs and manages <see cref="ActorSystem"/> in background of application.
    /// </summary>
    public class AkkaService : IHostedService
    {
        private ActorSystem ClusterSystem;
        private readonly IServiceProvider _serviceProvider;

        public AkkaService(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
             var config = ConfigurationFactory.ParseString(File.ReadAllText("app.conf")).BootstrapFromDocker();
             var bootstrap = BootstrapSetup.Create()
                .WithConfig(config) // load HOCON
                .WithActorRefProvider(ProviderSelection.Cluster.Instance); // launch Akka.Cluster

            // N.B. `WithActorRefProvider` isn't actually needed here - the HOCON file already specifies Akka.Cluster

            // enable DI support inside this ActorSystem, if needed
            var diSetup = ServiceProviderSetup.Create(_serviceProvider);

            // merge this setup (and any others) together into ActorSystemSetup
            var actorSystemSetup = bootstrap.And(diSetup);

            // start ActorSystem
            ClusterSystem = ActorSystem.Create("ClusterSys", actorSystemSetup);

            // start Petabridge.Cmd (https://cmd.petabridge.com/)
            var pbm = PetabridgeCmd.Get(ClusterSystem);
            pbm.RegisterCommandPalette(ClusterCommands.Instance);
            pbm.RegisterCommandPalette(RemoteCommands.Instance);
            pbm.Start(); // begin listening for PBM management commands

            // instantiate actors
            ClusterSystem.ActorOf(Props.Create(() => new DistributedPubSubTester()), "tester");

            // use the ServiceProvider ActorSystem Extension to start DI'd actors
            var sp = ServiceProvider.For(ClusterSystem);
            
            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            // strictly speaking this may not be necessary - terminating the ActorSystem would also work
            // but this call guarantees that the shutdown of the cluster is graceful regardless
             await CoordinatedShutdown.Get(ClusterSystem).Run(CoordinatedShutdown.ClrExitReason.Instance);
        }
    }
   
}
