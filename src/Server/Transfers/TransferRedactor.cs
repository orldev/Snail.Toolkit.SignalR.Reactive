using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;

namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// Turns user and session ids into what the logs may show.
/// </summary>
/// <param name="options">Whether real ids may be logged.</param>
/// <remarks>
/// A pseudonym is the start of an HMAC under a key drawn when the process starts and never stored, so the same id
/// reads the same within one run and nothing links it to the id itself, nor to the next run.
/// </remarks>
public sealed class TransferRedactor(IOptions<TransferLogOptions> options)
{
    private readonly byte[] _key = RandomNumberGenerator.GetBytes(32);

    private readonly bool _isRevealing = options.Value.RevealIdentities;

    /// <summary>
    /// Gets what a log line may say in place of an id.
    /// </summary>
    /// <param name="id">The user or session id.</param>
    /// <returns>The id itself when revealing is on, otherwise its pseudonym.</returns>
    public string Name(string? id)
    {
        if (string.IsNullOrEmpty(id))
            return "-";

        if (_isRevealing)
            return id;

        Span<byte> digest = stackalloc byte[32];
        HMACSHA256.HashData(_key, Encoding.UTF8.GetBytes(id), digest);

        return $"~{Convert.ToHexStringLower(digest[..4])}";
    }
}
