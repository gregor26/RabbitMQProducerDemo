using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQProducerDemo
{
    internal static class Program
    {
        private static async Task Main(string[] args)
        {
            const string queueName = "ha.telemetry";

            Console.WriteLine("RabbitMQ Producer Demo");

            try
            {
                var connectionFactory = new ConnectionFactory()
                {
                    HostName = "10.190.0.14",
                    UserName = "test",
                    Password = "test",
                    Port = 9101,
                    RequestedConnectionTimeout = TimeSpan.FromMilliseconds(3000), // milliseconds
                };

                using (var connection = connectionFactory.CreateConnection())
                {
                    using (var channel = connection.CreateModel())
                    {
                        // Declaring a queue is idempotent
                        channel.QueueDeclare(
                            queue: queueName,
                            durable: true,  // durable queue
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);

                        channel.ConfirmSelect(); // enable publisher confirms

                        //channel.BasicAcks += (sender, ea) =>
                        //{
                        //    Console.WriteLine(" [x] Ack");
                        //};
                        //channel.BasicNacks += (sender, ea) =>
                        //{
                        //    Console.WriteLine(" [x] Nack");
                        //};

                        CancellationTokenSource cts = new CancellationTokenSource();
                        Console.CancelKeyPress += (_, e) =>
                        {
                            e.Cancel = true; // prevent the process from terminating.
                            cts.Cancel();
                        };

                        while (!cts.Token.IsCancellationRequested)
                        {
                            string body = $"A nice random message: {DateTime.Now}";

                            var properties = channel.CreateBasicProperties();
                            properties.Persistent = true;
                            properties.Type = "abc";
                            properties.MessageId = Guid.NewGuid().ToString();

                            channel.BasicPublish(
                                exchange: string.Empty,
                                routingKey: queueName,
                                basicProperties: properties,
                                body: Encoding.UTF8.GetBytes(body));

                            // uses a 2 second timeout
                            channel.WaitForConfirmsOrDie(new TimeSpan(0, 0, 2));

                            Console.WriteLine(" [x] Sent: {0}", body);

                            await Task.Delay(1000, cts.Token);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.ToString());
                Console.ForegroundColor = ConsoleColor.White;
            }

            Console.WriteLine($"");
            Console.WriteLine($"Press any key to finish.");
            Console.Read();
        }
    }
}