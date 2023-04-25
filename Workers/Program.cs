using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Zeebe.Client;
using Zeebe.Client.Api.Responses;
using Zeebe.Client.Api.Worker;
using Zeebe.Client.Impl.Builder;
using NLog.Extensions.Logging;
using CamundaTraining.Services;
 
namespace CamundaTraining.Workers
{
    internal class Program
    {
        
        private static readonly string CLIENT_ID ="KqMiwG.25y.dbGMawihd1yYW2DAgn84y";
        private static readonly string CLIENT_SECRET ="F1k.57wag365J21bXMeF7zgglN_vMYxN5EKEPqfC_-mh9Tqpj47p_sDpU2PEN29g";
        private static readonly string ZEEBE_ADDRESS ="6f5078aa-c1ec-4698-b86a-f84e8536da63.bru-2.zeebe.camunda.io:443";
        private static readonly string JobCreditDeduction = "credit-deduction";
        private static readonly string JobChargeCreditCard = "credit-card-charging";
        private static readonly string WorkerName = Environment.MachineName;
        private static readonly long WorkCount = 100L;
 
        public static async Task Main(string[] args)
        {
            // create zeebe client
            var client = CamundaCloudClientBuilder
                .Builder()
                .UseClientId(CLIENT_ID)
                .UseClientSecret(CLIENT_SECRET)
                .UseContactPoint(ZEEBE_ADDRESS)
                .UseLoggerFactory(new NLogLoggerFactory())
                .Build();

            var topology = await client.TopologyRequest()
                .Send();
            Console.WriteLine(topology);
 
            // open job worker
            using (var signal = new EventWaitHandle(false, EventResetMode.AutoReset))
            {
                client.NewWorker()
                      .JobType(JobCreditDeduction)
                      .Handler(HandleJobCreditDeduction)
                      .MaxJobsActive(5)
                      .Name(WorkerName)
                      .AutoCompletion()
                      .PollInterval(TimeSpan.FromSeconds(1))
                      .Timeout(TimeSpan.FromSeconds(10))
                      .Open();

                client.NewWorker()
                      .JobType(JobChargeCreditCard)
                      .Handler(HandleJobChargeCreditCard)
                      .MaxJobsActive(5)
                      .Name(WorkerName)
                      .AutoCompletion()
                      .PollInterval(TimeSpan.FromSeconds(1))
                      .Timeout(TimeSpan.FromSeconds(10))
                      .Open();

                // blocks main thread, so that worker can run
                signal.WaitOne();
            }
             
        }

        private static void HandleJobCreditDeduction(IJobClient jobClient, IJob job)
        {
            // business logic
            var jobKey = job.Key;
            Console.WriteLine("Handling job: " + job);

            jobClient.NewCompleteJobCommand(jobKey)
                    .Variables("{\"foo\":2}")
                    .Send()
                    .GetAwaiter()
                    .GetResult();
            
        }
        private static void HandleJobChargeCreditCard(IJobClient jobClient, IJob job)
        {
            // business logic
            var jobKey = job.Key;
            Console.WriteLine("Managing job: " + job); 
            jobClient.NewCompleteJobCommand(jobKey)
                    .Variables("{\"foo\":2}")
                    .Send()
                    .GetAwaiter()
                    .GetResult();
            }
        
    }
}
