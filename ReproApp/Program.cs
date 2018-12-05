using System;

namespace ReproApp
{
    using System.Data.SqlClient;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Microsoft.Azure.ServiceBus.Management;

    class Program
    {
        static async Task Main(string[] args)
        {
            var connectionString = Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString");
            const string sqlConnectionString = Environment.GetEnvironmentVariable("SQLServer_ConnectionString");

            var management = new ManagementClient(connectionString);

            if (! await management.QueueExistsAsync("input"))
            {
                await management.CreateQueueAsync("input");
            }
            if (! await management.QueueExistsAsync("output"))
            {
                await management.CreateQueueAsync("output");
            }

            var message = new Message(Encoding.UTF8.GetBytes("test message"));
            var sender = new MessageSender(connectionString, "input");
            await sender.SendAsync(message);
            await sender.CloseAsync();

            // receive a message
            var connection = new ServiceBusConnection(connectionString);
            var receiver = new MessageReceiver(connection, "input");
            sender = new MessageSender(connection, entityPath:"output", viaEntityPath:"input");
            var tcs = new CancellationTokenSource(TimeSpan.FromSeconds(30));

            message = await receiver.ReceiveAsync();

            using (var tx = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                await sender.SendAsync(new Message(Encoding.Default.GetBytes("response")));
                await receiver.CompleteAsync(message.SystemProperties.LockToken);

                using (var suppress = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled))
                {
                    await ExecuteInnerTransaction(sqlConnectionString);

                    suppress.Complete();
                }

                tx.Complete();
            }

            await management.CloseAsync();
            await sender.CloseAsync();
            await receiver.CloseAsync();
        }

        private static async Task ExecuteInnerTransaction(string sqlConnectionString)
        {
            using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew, TransactionScopeAsyncFlowOption.Enabled))
            {
                using (var connection = new SqlConnection(sqlConnectionString))
                {
                    var command = new SqlCommand("INSERT INTO Test (Stamp) Values (@Stamp)");
                    command.Parameters.AddWithValue("@Stamp", DateTime.Now);
                    command.Connection = connection;

                    await connection.OpenAsync();
                    await command.ExecuteNonQueryAsync();
                }

                scope.Complete();
            }
        }
    }
}
