using System.Text;

namespace CosmosDB.InMemoryEmulator;

/// <summary>
/// MurmurHash3 (32-bit) implementation used for partition key range assignment.
/// Shared between <see cref="InMemoryContainer"/> and <see cref="FakeCosmosHandler"/>.
/// </summary>
internal static class PartitionKeyHash
{
	internal static uint MurmurHash3(string value)
	{
		var data = Encoding.UTF8.GetBytes(value);
		const uint seed = 0;
		const uint c1 = 0xcc9e2d51;
		const uint c2 = 0x1b873593;

		var hash = seed;
		var nblocks = data.Length / 4;

		for (var i = 0; i < nblocks; i++)
		{
			var k = BitConverter.ToUInt32(data, i * 4);
			k *= c1;
			k = RotateLeft(k, 15);
			k *= c2;
			hash ^= k;
			hash = RotateLeft(hash, 13);
			hash = hash * 5 + 0xe6546b64;
		}

		uint tail = 0;
		var tailStart = nblocks * 4;
		switch (data.Length & 3)
		{
			case 3: tail ^= (uint)data[tailStart + 2] << 16; goto case 2;
			case 2: tail ^= (uint)data[tailStart + 1] << 8; goto case 1;
			case 1:
				tail ^= data[tailStart];
				tail *= c1;
				tail = RotateLeft(tail, 15);
				tail *= c2;
				hash ^= tail;
				break;
		}

		hash ^= (uint)data.Length;
		hash = FMix(hash);
		return hash;
	}

	/// <summary>
	/// Returns the 0-based range index for a partition key value given N total ranges.
	/// </summary>
	internal static int GetRangeIndex(string partitionKeyValue, int rangeCount)
	{
		if (rangeCount <= 1) return 0;
		var hash = MurmurHash3(partitionKeyValue);
		// Use interval-based assignment matching FeedRange boundaries
		// which divide the uint32 hash space [0, 0x100000000) into N equal intervals
		var step = 0x1_0000_0000L / rangeCount;
		var index = (int)(hash / step);
		return Math.Min(index, rangeCount - 1);
	}

	/// <summary>
	/// Converts a 0-based range index to a hex boundary string for FeedRangeEpk.
	/// Range boundaries divide the uint32 hash space evenly and are encoded as uppercase hex.
	/// </summary>
	internal static string RangeBoundaryToHex(long boundary)
	{
		if (boundary <= 0) return "";
		if (boundary >= 0x1_0000_0000L) return "FF";
		return ((uint)boundary).ToString("X8");
	}

	private static uint RotateLeft(uint x, int r) => (x << r) | (x >> (32 - r));

	private static uint FMix(uint h)
	{
		h ^= h >> 16;
		h *= 0x85ebca6b;
		h ^= h >> 13;
		h *= 0xc2b2ae35;
		h ^= h >> 16;
		return h;
	}
}
