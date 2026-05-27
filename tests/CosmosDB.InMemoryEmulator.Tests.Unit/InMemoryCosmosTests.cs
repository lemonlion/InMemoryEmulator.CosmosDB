using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class InMemoryCosmosCreateTests
{
    [Fact]
    public async Task Create_SimplestCase_ReturnsWorkingContainer()
    {
        using var cosmos = InMemoryCosmos.Create("orders");
        await cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "1", Name = "Widget" },
            new PartitionKey("1"));
        var response = await cosmos.Container.ReadItemAsync<TestDocument>("1", new PartitionKey("1"));
        response.Resource.Name.Should().Be("Widget");
    }

    [Fact]
    public async Task Create_WithCustomPartitionKey_Works()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");
        await cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));
        var response = await cosmos.Container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        response.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task Create_WithHierarchicalPartitionKey_Works()
    {
        using var cosmos = InMemoryCosmos.Create("events", new[] { "/tenantId", "/category" });
        var item = new { id = "1", tenantId = "t1", category = "c1", name = "ev" };
        var pk = new PartitionKeyBuilder().Add("t1").Add("c1").Build();
        await cosmos.Container.CreateItemAsync(item, pk);
        var response = await cosmos.Container.ReadItemAsync<JObject>("1", pk);
        response.Resource["name"]!.ToString().Should().Be("ev");
    }

    [Fact]
    public void Create_ContainerProperty_ReturnsSdkContainer()
    {
        using var cosmos = InMemoryCosmos.Create("orders");
        cosmos.Container.Should().NotBeNull();
        cosmos.Container.Should().BeAssignableTo<Container>();
    }

    [Fact]
    public void Create_ContainersDictionary_ContainsSingleEntry()
    {
        using var cosmos = InMemoryCosmos.Create("orders");
        cosmos.Containers.Should().HaveCount(1);
        cosmos.Containers.Should().ContainKey("orders");
        cosmos.Containers["orders"].Should().BeSameAs(cosmos.Container);
    }

    [Fact]
    public void Create_HandlerProperty_ReturnsFakeCosmosHandler()
    {
        using var cosmos = InMemoryCosmos.Create("orders");
        cosmos.Handler.Should().NotBeNull();
        cosmos.Handler.Should().BeOfType<FakeCosmosHandler>();
    }

    [Fact]
    public void Create_HandlersDictionary_ContainsSingleEntry()
    {
        using var cosmos = InMemoryCosmos.Create("orders");
        cosmos.Handlers.Should().HaveCount(1);
        cosmos.Handlers.Should().ContainKey("orders");
        cosmos.Handlers["orders"].Should().BeSameAs(cosmos.Handler);
    }

    [Fact]
    public void Create_ClientProperty_ReturnsCosmosClient()
    {
        using var cosmos = InMemoryCosmos.Create("orders");
        cosmos.Client.Should().NotBeNull();
        cosmos.Client.Should().BeAssignableTo<CosmosClient>();
    }

    [Fact]
    public void Create_DefaultDatabaseName_IsDefault()
    {
        InMemoryCosmos.DefaultDatabaseName.Should().Be("default");
    }

    [Fact]
    public async Task Create_ClientGetContainer_WorksWithDefaultDatabase()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");
        var container = cosmos.Client.GetContainer(InMemoryCosmos.DefaultDatabaseName, "orders");
        await container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));
        var response = await container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        response.Resource.Name.Should().Be("Test");
    }
}

public class InMemoryCosmosCreateWithOptionsTests
{
    [Fact]
    public async Task Create_WithWrapHandler_WrapsTheHandler()
    {
        var wasCalled = false;
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey",
            wrapHandler: h =>
            {
                wasCalled = true;
                return new TrackingDelegatingHandler(h);
            });

        await cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        wasCalled.Should().BeTrue();
    }

    [Fact]
    public void Create_WithConfigureContainer_ConfiguresContainer()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey",
            configureContainer: c => c.DefaultTimeToLive = 3600);

        // The setup container should reflect the configuration
        var setup = cosmos.SetupContainer();
        setup.Should().NotBeNull();
    }

    [Fact]
    public async Task Create_WithConfigureOptions_AppliesOptions()
    {
        // Just verifying it doesn't throw - configureOptions runs on CosmosClientOptions
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey",
            configureOptions: o => o.MaxRetryAttemptsOnRateLimitedRequests = 0);

        await cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));
    }

    [Fact]
    public void Create_WithConfigureOptions_RejectsHttpClientFactory()
    {
        var act = () => InMemoryCosmos.Create("orders", "/partitionKey",
            configureOptions: o => o.HttpClientFactory = () => new HttpClient());

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*HttpClientFactory*wrapHandler*");
    }

    private class TrackingDelegatingHandler : DelegatingHandler
    {
        public TrackingDelegatingHandler(HttpMessageHandler inner) : base(inner) { }
    }
}

public class InMemoryCosmosBuilderTests
{
    [Fact]
    public async Task Builder_MultiContainer_Works()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("customers", "/id")
            .Build();

        await cosmos.Containers["orders"].CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Order1" },
            new PartitionKey("pk1"));

        await cosmos.Containers["customers"].CreateItemAsync(
            new { id = "c1", name = "Alice" },
            new PartitionKey("c1"));

        var orderResponse = await cosmos.Containers["orders"].ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        orderResponse.Resource.Name.Should().Be("Order1");
    }

    [Fact]
    public void Builder_ContainersDictionary_HasAllContainers()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("customers", "/id")
            .Build();

        cosmos.Containers.Should().HaveCount(2);
        cosmos.Containers.Should().ContainKey("orders");
        cosmos.Containers.Should().ContainKey("customers");
    }

    [Fact]
    public void Builder_HandlersDictionary_HasAllHandlers()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("customers", "/id")
            .Build();

        cosmos.Handlers.Should().HaveCount(2);
        cosmos.Handlers.Should().ContainKey("orders");
        cosmos.Handlers.Should().ContainKey("customers");
    }

    [Fact]
    public void Builder_ContainerSingular_ThrowsForMulti()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("customers", "/id")
            .Build();

        var act = () => cosmos.Container;

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*single-container*");
    }

    [Fact]
    public void Builder_HandlerSingular_ThrowsForMulti()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("customers", "/id")
            .Build();

        var act = () => cosmos.Handler;

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*single-container*");
    }

    [Fact]
    public void Builder_WithHierarchicalPk_Works()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("events", new[] { "/tenantId", "/category" })
            .Build();

        cosmos.Containers.Should().ContainKey("events");
    }

    [Fact]
    public void Builder_WrapHandler_WrapsAllHandlers()
    {
        var callCount = 0;
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("customers", "/id")
            .WrapHandler(h =>
            {
                Interlocked.Increment(ref callCount);
                return h;
            })
            .Build();

        // wrapHandler is called once per container handler (or once for the router)
        callCount.Should().BeGreaterThan(0);
    }

    [Fact]
    public void Builder_ConfigureOptions_AppliesOptions()
    {
        // Should not throw
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .ConfigureOptions(o => o.MaxRetryAttemptsOnRateLimitedRequests = 0)
            .Build();
    }

    [Fact]
    public void Builder_AddContainerWithConfigure_ConfiguresContainer()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey", container =>
            {
                container.DefaultTimeToLive = 3600;
            })
            .Build();

        // Container was configured
        cosmos.SetupContainer("orders").Should().NotBeNull();
    }

    [Fact]
    public void Builder_BuildMultipleTimes_CreatesIndependentResults()
    {
        var builder = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey");

        using var cosmos1 = builder.Build();
        using var cosmos2 = builder.Build();

        cosmos1.Container.Should().NotBeSameAs(cosmos2.Container);
        cosmos1.Handler.Should().NotBeSameAs(cosmos2.Handler);
    }
}

public class InMemoryCosmosValidationTests
{
    [Fact]
    public void Builder_BuildWithNoContainers_Throws()
    {
        var builder = InMemoryCosmos.Builder();

        var act = () => builder.Build();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*At least one container*");
    }

    [Fact]
    public void Builder_DuplicateContainerName_Throws()
    {
        var act = () => InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("orders", "/id");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Container 'orders' has already been added*");
    }

    [Fact]
    public void Create_NullContainerName_Throws()
    {
        var act = () => InMemoryCosmos.Create(null!);
        act.Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Create_EmptyOrWhitespaceContainerName_Throws(string name)
    {
        var act = () => InMemoryCosmos.Create(name);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Builder_AddContainer_NullName_Throws()
    {
        var act = () => InMemoryCosmos.Builder().AddContainer(null!, "/id");
        act.Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Builder_AddContainer_EmptyOrWhitespaceName_Throws(string name)
    {
        var act = () => InMemoryCosmos.Builder().AddContainer(name, "/id");
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Create_PkPathNotStartingWithSlash_Throws()
    {
        var act = () => InMemoryCosmos.Create("orders", "customerId");

        act.Should().Throw<ArgumentException>()
            .WithMessage("*Partition key path must start with*");
    }

    [Fact]
    public void Builder_AddContainer_PkPathNotStartingWithSlash_Throws()
    {
        var act = () => InMemoryCosmos.Builder().AddContainer("orders", "customerId");

        act.Should().Throw<ArgumentException>()
            .WithMessage("*Partition key path must start with*");
    }

    [Fact]
    public void Create_EmptyPartitionKeyPathsArray_Throws()
    {
        var act = () => InMemoryCosmos.Create("orders", Array.Empty<string>());
        act.Should().Throw<ArgumentException>();
    }
}

public class InMemoryCosmosSetupContainerTests
{
    [Fact]
    public void SetupContainer_SingleContainer_NoArg_ReturnsSetup()
    {
        using var cosmos = InMemoryCosmos.Create("orders");
        var setup = cosmos.SetupContainer();
        setup.Should().NotBeNull();
        setup.Should().BeAssignableTo<IContainerTestSetup>();
    }

    [Fact]
    public void SetupContainer_SingleContainer_ByName_ReturnsSetup()
    {
        using var cosmos = InMemoryCosmos.Create("orders");
        var setup = cosmos.SetupContainer("orders");
        setup.Should().NotBeNull();
    }

    [Fact]
    public void SetupContainer_MultiContainer_NoArg_Throws()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("customers", "/id")
            .Build();

        var act = () => cosmos.SetupContainer();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*SetupContainer()*single-container*");
    }

    [Fact]
    public void SetupContainer_MultiContainer_ByName_Works()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("customers", "/id")
            .Build();

        var ordersSetup = cosmos.SetupContainer("orders");
        ordersSetup.Should().NotBeNull();

        var customersSetup = cosmos.SetupContainer("customers");
        customersSetup.Should().NotBeNull();
    }

    [Fact]
    public void SetupContainer_NonexistentName_Throws()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .Build();

        var act = () => cosmos.SetupContainer("nonexistent");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Container 'nonexistent' not found*Available containers*orders*");
    }

    [Fact]
    public void GetHandler_NonexistentName_Throws()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .Build();

        var act = () => cosmos.GetHandler("nonexistent");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Container 'nonexistent' not found*Available containers*orders*");
    }
}

public class InMemoryCosmosSetupOperationsTests
{
    [Fact]
    public void SetupContainer_RegisterStoredProcedure_Works()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

        // Should not throw - verifies registration through IContainerTestSetup
        cosmos.SetupContainer().RegisterStoredProcedure("mysproc",
            (pk, args) => "{\"result\":\"ok\"}");
    }

    [Fact]
    public async Task SetupContainer_RegisterUdf_Works()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

        cosmos.SetupContainer().RegisterUdf("toUpper",
            args => ((string)args[0]!).ToUpper());

        // UDF registered — can be used in queries
        await cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "hello" },
            new PartitionKey("pk1"));

        var query = cosmos.Container.GetItemQueryIterator<JObject>(
            "SELECT udf.toUpper(c.name) AS upper FROM c");
        var results = new List<JObject>();
        while (query.HasMoreResults)
        {
            var page = await query.ReadNextAsync();
            results.AddRange(page);
        }
        results.Should().NotBeEmpty();
        results[0]["upper"]!.ToString().Should().Be("HELLO");
    }

    [Fact]
    public void SetupContainer_RegisterTrigger_Works()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

        // Should not throw
        cosmos.SetupContainer().RegisterTrigger("audit",
            TriggerType.Pre, TriggerOperation.All,
            doc => { doc["audited"] = true; return doc; });
    }

    [Fact]
    public void SetupContainer_ClearItems_Works()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

        cosmos.SetupContainer().ClearItems();
        // Should not throw - just verifying the method exists and works
    }

    [Fact]
    public void ClearItems_ConvenienceMethod_Works()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

        cosmos.ClearItems();
        // Should not throw - convenience method on InMemoryCosmosResult
    }

    [Fact]
    public async Task SetupContainer_ExportImportState_Works()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

        await cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var state = cosmos.ExportState();
        state.Should().Contain("\"id\"");
        state.Should().Contain("Test");

        cosmos.ClearItems();
        cosmos.ImportState(state);

        var response = await cosmos.Container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        response.Resource.Name.Should().Be("Test");
    }

    [Fact]
    public async Task ExportImportState_NamedContainer_Works()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("products", "/categoryId")
            .Build();

        await cosmos.Containers["orders"].CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Order1" },
            new PartitionKey("pk1"));

        var state = cosmos.ExportState("orders");
        state.Should().Contain("Order1");

        cosmos.ClearItems("orders");
        cosmos.ImportState(state, "orders");

        var response = await cosmos.Containers["orders"].ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        response.Resource.Name.Should().Be("Order1");
    }

    [Fact]
    public void SetupContainer_JsStoredProcedure_WithoutEngine_ThrowsHelpful()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

        var act = () => cosmos.SetupContainer().RegisterStoredProcedure("mysproc",
            "function(prefix) { var context = getContext(); }");

        act.Should().Throw<NotImplementedException>()
            .WithMessage("*JsTriggers*");
    }

    [Fact]
    public void SetupContainer_JsUdf_WithoutEngine_ThrowsHelpful()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

        var act = () => cosmos.SetupContainer().RegisterUdf("toUpper",
            "function(s) { return s.toUpperCase(); }");

        act.Should().Throw<NotImplementedException>()
            .WithMessage("*JsTriggers*");
    }

    [Fact]
    public void SetupContainer_JsTrigger_WithoutEngine_ThrowsHelpful()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

        var act = () => cosmos.SetupContainer().RegisterTrigger("validate",
            TriggerType.Pre, TriggerOperation.Create,
            "function() { var context = getContext(); }");

        act.Should().Throw<NotImplementedException>()
            .WithMessage("*JsTriggers*");
    }
}

public class InMemoryCosmosFaultInjectionTests
{
    [Fact]
    public async Task Handler_FaultInjector_Works()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

        cosmos.Handler.FaultInjector = req =>
        {
            if (req.Method == HttpMethod.Post)
                return new HttpResponseMessage((HttpStatusCode)429);
            return null;
        };

        var act = () => cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var exception = await act.Should().ThrowAsync<CosmosException>();
        exception.Which.StatusCode.Should().Be((HttpStatusCode)429);
    }

    [Fact]
    public void SetFaultInjector_SetsOnAllHandlers()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("customers", "/id")
            .Build();

        Func<HttpRequestMessage, HttpResponseMessage?> injector =
            req => new HttpResponseMessage((HttpStatusCode)503);

        cosmos.SetFaultInjector(injector);

        cosmos.Handlers["orders"].FaultInjector.Should().BeSameAs(injector);
        cosmos.Handlers["customers"].FaultInjector.Should().BeSameAs(injector);
    }

    [Fact]
    public void SetFaultInjector_Null_ClearsAllHandlers()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("customers", "/id")
            .Build();

        cosmos.SetFaultInjector(req => new HttpResponseMessage((HttpStatusCode)503));
        cosmos.SetFaultInjector(null);

        cosmos.Handlers["orders"].FaultInjector.Should().BeNull();
        cosmos.Handlers["customers"].FaultInjector.Should().BeNull();
    }

    [Fact]
    public void GetHandler_ReturnsCorrectHandler()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("orders", "/partitionKey")
            .AddContainer("customers", "/id")
            .Build();

        var handler = cosmos.GetHandler("orders");
        handler.Should().BeSameAs(cosmos.Handlers["orders"]);
    }
}

public class InMemoryCosmosDisposeTests
{
    [Fact]
    public void Dispose_DoesNotThrow()
    {
        var cosmos = InMemoryCosmos.Create("orders");
        cosmos.Dispose();
        // Should not throw
    }

    [Fact]
    public async Task AsyncDispose_DoesNotThrow()
    {
        var cosmos = InMemoryCosmos.Create("orders");
        await cosmos.DisposeAsync();
        // Should not throw
    }

    [Fact]
    public void Create_MultipleIndependentResults()
    {
        using var cosmos1 = InMemoryCosmos.Create("orders");
        using var cosmos2 = InMemoryCosmos.Create("orders");

        // Independent results — different instances
        cosmos1.Container.Should().NotBeSameAs(cosmos2.Container);
        cosmos1.Handler.Should().NotBeSameAs(cosmos2.Handler);
    }
}

public class InMemoryCosmosContainerCasingTests
{
    [Fact]
    public void ContainersDictionary_IsCaseSensitive()
    {
        using var cosmos = InMemoryCosmos.Builder()
            .AddContainer("Orders", "/partitionKey")
            .Build();

        cosmos.Containers.Should().ContainKey("Orders");
        cosmos.Containers.Keys.Should().NotContain("orders");
    }
}

public class InMemoryCosmosQueryLogTests
{
    [Fact]
    public async Task Handler_QueryLog_RecordsQueries()
    {
        using var cosmos = InMemoryCosmos.Create("orders", "/partitionKey");

        await cosmos.Container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Test" },
            new PartitionKey("pk1"));

        var query = cosmos.Container.GetItemQueryIterator<TestDocument>("SELECT * FROM c");
        while (query.HasMoreResults) await query.ReadNextAsync();

        cosmos.Handler.QueryLog.Should().NotBeEmpty();
    }
}
