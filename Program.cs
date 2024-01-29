using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<IConnection>(_ => 
    new ConnectionFactory()
    {
        Uri = new Uri(@"amqp://guest:guest@localhost:5672"),
        NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
        AutomaticRecoveryEnabled = true
    }.CreateConnection());

builder.Services.AddSingleton<IMessageBus, RabbitMqMessageBus>();
builder.Services.AddHostedService<OrdersSubscriber>();
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer("Server=localhost;Database=checkout;User Id=sa;Password=password123!;Persist Security Info=False;Encrypt=False;TrustServerCertificate=False;MultipleActiveResultSets=True;"));

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.UseHttpsRedirection();

var orders = app.MapGroup("/orders");
orders.MapPost("", async (OrderProcessedEvent @event, IMessageBus bus) =>
{
    await bus.Publish("order.processed", @event);
    return TypedResults.Accepted<Out>("", new Out("Order accepted"));
});

app.Run();

// app db context
class Order
{
    public int Id { get; set; }
    public string OrderId { get; set; }
    public string CustomerId { get; set; }
    public string ProductId { get; set; }
    public int Quantity { get; set; }
    public OrderStatus Status { get; set; }
}
    
class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options, ILogger<AppDbContext> _logger) : base(options)
    {
        if (Database.EnsureCreated())
            _logger.LogInformation("Database created");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        
        // we can use the 
        // modelBuilder.ApplyConfiguration() that receives a class that implements IEntityTypeConfiguration<T>
        // or we can use the modelBuilder.Entity<T> that receives a lambda expression
        modelBuilder.Entity<Order>().HasKey(o => o.OrderId);
    }

    public DbSet<Order> Orders { get; set; }
}

// background service
public class OrdersSubscriber : BackgroundService
{
    private readonly IChannel _readChannel;
    private readonly ILogger<OrdersSubscriber> _logger;
    private readonly IServiceScopeFactory _scopefactory;

    public OrdersSubscriber(IConnection conn, ILogger<OrdersSubscriber> logger, IServiceScopeFactory scopefactory)
    {
        _readChannel = conn.CreateChannel();
        _logger = logger;
        _scopefactory = scopefactory;
        
        _readChannel.QueueDeclare("checkout.orders", true, false, false, null);
        _readChannel.QueueBind("checkout.orders", "checkout.orders", "order.*");
        logger.LogInformation("RabbitMQ settup completed");
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumer = new EventingBasicConsumer(_readChannel);
        consumer.Received += async (ch, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var routingKey = ea.RoutingKey;
                _logger.LogInformation("Received message {Message} with routing key {RoutingKey}", message, routingKey);
                using var scope = _scopefactory.CreateScope();
                
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                var @event = JsonSerializer.Deserialize<OrderProcessedEvent>(message);
                var order = await db.Orders.FirstOrDefaultAsync(o => o.OrderId == @event.OrderId);
                if (order == null)
                {
                    order = new Order
                    {
                        OrderId = @event.OrderId,
                        CustomerId = @event.CustomerId,
                        ProductId = @event.ProductId,
                        Quantity = @event.Quantity,
                        Status = @event.Status
                    };
                    await db.Orders.AddAsync(order);
                }
                else
                {
                    order.Status = @event.Status;
                }

                await db.SaveChangesAsync();
                _logger.LogInformation("Order {OrderId} processed", @event.OrderId);
                
                await _readChannel.BasicAckAsync(ea.DeliveryTag, false);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Unable to process message");
                await _readChannel.BasicNackAsync(ea.DeliveryTag, false, true);
                throw;
            }
        };
        
        _logger.LogInformation("Subscribing to RabbitMQ");
        _readChannel.BasicConsume("checkout.orders", false, consumer);
        return Task.CompletedTask;
    }
}

// message bus
interface IMessageBus
{
    Task Publish<T>(string eventName, T message);
}

class RabbitMqMessageBus : IMessageBus
{
    private readonly IChannel _writeChannel; 
    private readonly ILogger<RabbitMqMessageBus> _logger;
    
    public RabbitMqMessageBus(ILogger<RabbitMqMessageBus> logger, IConnection conn)
    {
        _logger = logger;
        _writeChannel = conn.CreateChannel();
        _writeChannel.ExchangeDeclare("checkout.orders", ExchangeType.Topic, true, false);
        _writeChannel.ConfirmSelect();
        _logger.LogInformation("RabbitMQ settup completed");
    }
    
    public async Task Publish<T>(string eventName, T message)
    {
        try
        {
            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));
            await _writeChannel.BasicPublishAsync(
                exchange: "checkout.orders",
                routingKey: eventName,
                body: body);
            await _writeChannel.WaitForConfirmsOrDieAsync();
            _logger.LogInformation("Message published to RabbitMQ {@Message}", message);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Unable to publish message to RabbitMQ");
            throw;
        }
    }
}

enum OrderStatus
{
    New,
    Processing,
    Processed,
    Cancelled,
    Declined,
    Refunded
}

// event
record OrderProcessedEvent(string OrderId, string CustomerId, string ProductId, int Quantity, OrderStatus Status);
record Out(string Msg);
