using AwesomeAssertions;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

public class BuildSelectTokenPathTests
{
	[Fact]
	public void NoSegments_ReturnsEmpty()
	{
		InMemoryContainer.BuildSelectTokenPath(Array.Empty<string>())
			.Should().BeEmpty();
	}

	[Fact]
	public void SingleNameSegment_ReturnsName()
	{
		InMemoryContainer.BuildSelectTokenPath(new[] { "name" })
			.Should().Be("name");
	}

	[Fact]
	public void SingleNumericSegment_ReturnsBracketNotation()
	{
		InMemoryContainer.BuildSelectTokenPath(new[] { "0" })
			.Should().Be("[0]");
	}

	[Fact]
	public void MixedSegments_CorrectNotation()
	{
		InMemoryContainer.BuildSelectTokenPath(new[] { "items", "0", "name" })
			.Should().Be("items[0].name");
	}

	[Fact]
	public void ConsecutiveNumericSegments_CorrectNotation()
	{
		InMemoryContainer.BuildSelectTokenPath(new[] { "matrix", "0", "1" })
			.Should().Be("matrix[0][1]");
	}

	[Fact]
	public void AllNameSegments_DotSeparated()
	{
		InMemoryContainer.BuildSelectTokenPath(new[] { "a", "b", "c" })
			.Should().Be("a.b.c");
	}

	[Fact]
	public void NumericFirst_BracketNotation()
	{
		InMemoryContainer.BuildSelectTokenPath(new[] { "0", "name" })
			.Should().Be("[0].name");
	}

	[Fact]
	public void TrailingNumeric_CorrectNotation()
	{
		InMemoryContainer.BuildSelectTokenPath(new[] { "items", "0" })
			.Should().Be("items[0]");
	}
}
