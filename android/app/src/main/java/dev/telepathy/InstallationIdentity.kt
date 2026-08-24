package dev.telepathy

import android.content.Context
import android.content.SharedPreferences
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import java.security.KeyStore
import java.security.SecureRandom
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

/**
 * Stable, opaque ownership for one app installation.
 *
 * This is intentionally separate from the human-readable hello device label:
 * the bridge uses this value to decide which handset owns a durable receipt.
 */
internal object InstallationIdentity {
    const val MAX_LENGTH = 128

    internal enum class SentinelState {
        Present,
        Missing,
    }

    private const val PREFS_NAME = "telepathy_identity"
    private const val KEY_INSTALLATION_ID = "installation_id"
    private const val KEY_SENTINEL_MARKER = "keystore_sentinel_marker"
    private const val KEYSTORE_PROVIDER = "AndroidKeyStore"
    private const val KEYSTORE_SENTINEL_ALIAS = "telepathy_installation_sentinel_v1"
    private const val SENTINEL_TRANSFORMATION = "AES/GCM/NoPadding"
    private const val SENTINEL_IV_LENGTH_BYTES = 12
    private const val SENTINEL_TAG_LENGTH_BITS = 128
    private const val SENTINEL_PLAINTEXT_PREFIX = "telepathy-installation-sentinel-v1\u0000"
    private const val RANDOM_BYTES = 16
    private val lock = Any()
    private val secureRandom = SecureRandom()

    fun getOrCreate(context: Context): String {
        val preferences = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        synchronized(lock) {
            // A restored SharedPreferences file can contain a valid-looking
            // owner while the original Android Keystore state is absent.  Do
            // not reuse that owner: the missing sentinel is the installation
            // boundary, so rotate the owner before any receipt is authorized.
            val sentinelState = ensureSentinel(preferences)
            return loadOrCreate(
                current = preferences.getString(KEY_INSTALLATION_ID, null),
                sentinelState = sentinelState,
                generate = { generate() },
                persist = { value ->
                    preferences.edit()
                        .putString(KEY_INSTALLATION_ID, value)
                        .putString(KEY_SENTINEL_MARKER, createSentinelMarker(value))
                        .commit()
                },
            )
        }
    }

    /** Pure persistence policy, kept injectable for JVM tests without a device context. */
    internal fun loadOrCreate(
        current: String?,
        sentinelState: SentinelState = SentinelState.Present,
        generate: () -> String = { InstallationIdentity.generate() },
        persist: (String) -> Boolean,
    ): String {
        if (!shouldGenerate(current, sentinelState)) return checkNotNull(current)

        val candidate = generate()
        check(isValid(candidate)) { "generated installation ID is invalid" }
        check(persist(candidate)) { "could not persist installation ID" }
        return candidate
    }

    /** Pure policy: a valid stored owner is reusable only with its device sentinel. */
    internal fun shouldGenerate(current: String?, sentinelState: SentinelState): Boolean =
        !isValid(current) || sentinelState == SentinelState.Missing

    /**
     * Create the sentinel on first use, but report that it was missing so a
     * copied valid SharedPreferences owner is rotated.  Android Keystore is
     * intentionally outside app backup/restore and its key material is tied
     * to the device's Keystore installation.
     */
    private fun ensureSentinel(preferences: SharedPreferences): SentinelState {
        val keyStore = openKeyStore()
        if (keyStore.containsAlias(KEYSTORE_SENTINEL_ALIAS)) {
            val key = loadSentinelKey(keyStore)
            return if (
                isValidSentinelMarker(
                    preferences.getString(KEY_SENTINEL_MARKER, null),
                    preferences.getString(KEY_INSTALLATION_ID, null),
                    key,
                )
            ) {
                SentinelState.Present
            } else {
                SentinelState.Missing
            }
        } else {
            KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, KEYSTORE_PROVIDER).apply {
                init(
                    KeyGenParameterSpec.Builder(
                        KEYSTORE_SENTINEL_ALIAS,
                        KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT,
                    )
                        .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                        .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                        .setRandomizedEncryptionRequired(true)
                        .build(),
                )
                generateKey()
            }
            // A newly created key intentionally reports Missing.  The caller
            // must persist a new marker and owner together, so a copied owner
            // cannot become valid merely because this install made a key.
            return SentinelState.Missing
        }
    }

    private fun openKeyStore(): KeyStore = try {
        KeyStore.getInstance(KEYSTORE_PROVIDER).apply { load(null) }
    } catch (error: Exception) {
        throw IllegalStateException("could not open Android Keystore", error)
    }

    private fun loadSentinelKey(keyStore: KeyStore): SecretKey =
        keyStore.getKey(KEYSTORE_SENTINEL_ALIAS, null) as? SecretKey
            ?: error("Android Keystore installation sentinel is unusable")

    private fun createSentinelMarker(owner: String): String {
        check(isValid(owner)) { "cannot bind Android Keystore sentinel to an invalid owner" }
        val key = loadSentinelKey(openKeyStore())
        val cipher = Cipher.getInstance(SENTINEL_TRANSFORMATION)
        cipher.init(Cipher.ENCRYPT_MODE, key)
        val encrypted = cipher.doFinal(sentinelPlaintext(owner))
        return Base64.getUrlEncoder().withoutPadding()
            .encodeToString(cipher.iv + encrypted)
    }

    private fun isValidSentinelMarker(marker: String?, owner: String?, key: SecretKey): Boolean {
        if (marker == null || !isValid(owner)) return false
        val encoded = runCatching { Base64.getUrlDecoder().decode(marker) }.getOrNull() ?: return false
        if (encoded.size <= SENTINEL_IV_LENGTH_BYTES) return false
        val iv = encoded.copyOfRange(0, SENTINEL_IV_LENGTH_BYTES)
        val encrypted = encoded.copyOfRange(SENTINEL_IV_LENGTH_BYTES, encoded.size)
        return runCatching {
            val cipher = Cipher.getInstance(SENTINEL_TRANSFORMATION)
            cipher.init(
                Cipher.DECRYPT_MODE,
                key,
                GCMParameterSpec(SENTINEL_TAG_LENGTH_BITS, iv),
            )
            cipher.doFinal(encrypted).contentEquals(sentinelPlaintext(checkNotNull(owner)))
        }.getOrDefault(false)
    }

    private fun sentinelPlaintext(owner: String): ByteArray =
        "$SENTINEL_PLAINTEXT_PREFIX$owner".toByteArray(Charsets.UTF_8)

    internal fun generate(): String {
        val bytes = ByteArray(RANDOM_BYTES)
        synchronized(lock) { secureRandom.nextBytes(bytes) }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
    }

    fun isValid(value: String?): Boolean =
        value != null &&
            value.isNotEmpty() &&
            value.length <= MAX_LENGTH &&
            !isProtocolBlank(value) &&
            value.isWellFormedUtf16() &&
            value.none { it.code in 0x00..0x1f || it.code in 0x7f..0x9f }
}
