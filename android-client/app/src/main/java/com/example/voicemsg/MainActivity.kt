package com.example.voicemsg

import android.Manifest
import android.content.pm.PackageManager
import android.media.*
import android.media.audiofx.AcousticEchoCanceler
import android.media.audiofx.AutomaticGainControl
import android.media.audiofx.NoiseSuppressor
import android.os.Bundle
import android.text.Editable
import android.text.TextWatcher
import android.view.View
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ProgressBar
import android.widget.TextView
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import com.google.android.material.chip.Chip
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.abs
import kotlin.math.min
import kotlin.random.Random

class MainActivity : AppCompatActivity() {
    private lateinit var editName: EditText
    private lateinit var editIp: EditText
    private lateinit var editPort: EditText
    private lateinit var editStream: EditText
    private lateinit var btnStart: Button
    private lateinit var btnStop: Button
    private lateinit var chipStatus: Chip
    private lateinit var progressMic: ProgressBar
    private lateinit var participantsContainer: LinearLayout

    private val running = AtomicBoolean(false)
    private var ioThread: Thread? = null
    private var recvThread: Thread? = null

    private var clientId: Int = Random.nextInt()
    @Volatile private var presenceReceived: Boolean = false
    @Volatile private var rosterReceived: Boolean = false

    // Track last time each participant spoke to toggle green dot reliably
    private val speakingTimestamps = mutableMapOf<Int, Long>()
    private val speakingDecayIntervalMs = 200L
    private val decayRunnable = object : Runnable {
        override fun run() {
            if (running.get()) {
                synchronized(speakingTimestamps) {
                    updateSpeakingFromTimestamps(speakingTimestamps.toMap())
                }
                // schedule next tick
                window?.decorView?.postDelayed(this, speakingDecayIntervalMs)
            }
        }
    }

    private val requestPermission = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { granted ->
        if (granted) startStreaming() else updateStatus("Permission denied")
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        editName = findViewById(R.id.editName)
        editIp = findViewById(R.id.editIp)
        editPort = findViewById(R.id.editPort)
        editStream = findViewById(R.id.editStream)
        btnStart = findViewById(R.id.btnStart)
        btnStop = findViewById(R.id.btnStop)
        chipStatus = findViewById(R.id.chipStatus)
        progressMic = findViewById(R.id.progressMic)
        participantsContainer = findViewById(R.id.participantsContainer)

        editIp.setText("192.168.1.67")
        editPort.setText("50010")
        editStream.setText("1")
        val defaultName = "User${abs(clientId % 1000)}"
        val prefs = getSharedPreferences("voicemsg_prefs", MODE_PRIVATE)
        val savedName = prefs.getString("display_name", null) ?: defaultName
        editName.setText(savedName)
        editName.addTextChangedListener(object : TextWatcher {
            override fun afterTextChanged(s: Editable?) {
                val name = s?.toString()?.trim().orEmpty()
                val toStore = if (name.isBlank()) defaultName else name
                prefs.edit().putString("display_name", toStore).apply()
            }
            override fun beforeTextChanged(s: CharSequence?, start: Int, count: Int, after: Int) {}
            override fun onTextChanged(s: CharSequence?, start: Int, before: Int, count: Int) {}
        })

        btnStart.setOnClickListener { ensurePermissionAndStart() }
        btnStop.setOnClickListener { stopStreaming() }
        setUiConnected(false)
    }

    private fun ensurePermissionAndStart() {
        val has = ContextCompat.checkSelfPermission(this, Manifest.permission.RECORD_AUDIO)
        if (has == PackageManager.PERMISSION_GRANTED) startStreaming()
        else requestPermission.launch(Manifest.permission.RECORD_AUDIO)
    }

    private fun startStreaming() {
        if (running.get()) return
        running.set(true)
        updateStatus("Подключение…")

        val serverIp = editIp.text.toString().trim()
        val serverPort = editPort.text.toString().toIntOrNull() ?: 50010
        val streamId = editStream.text.toString().toIntOrNull() ?: 1
        val displayName = editName.text.toString().ifBlank { "User${abs(clientId % 1000)}" }
        // Persist chosen display name
        try {
            getSharedPreferences("voicemsg_prefs", MODE_PRIVATE)
                .edit()
                .putString("display_name", displayName)
                .apply()
        } catch (_: Exception) {}

        ioThread = Thread {
            val sampleRate = 48000
            val channelConfigIn = AudioFormat.CHANNEL_IN_MONO
            val channelConfigOut = AudioFormat.CHANNEL_OUT_MONO
            val audioFormat = AudioFormat.ENCODING_PCM_16BIT

            val minBufIn = AudioRecord.getMinBufferSize(sampleRate, channelConfigIn, audioFormat)
            val minBufOut = AudioTrack.getMinBufferSize(sampleRate, channelConfigOut, audioFormat)

            val recorder = AudioRecord(
                MediaRecorder.AudioSource.VOICE_COMMUNICATION,
                sampleRate,
                channelConfigIn,
                audioFormat,
                minBufIn * 2
            )
            val track = AudioTrack(
                AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_VOICE_COMMUNICATION)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
                    .build(),
                AudioFormat.Builder()
                    .setSampleRate(sampleRate)
                    .setChannelMask(channelConfigOut)
                    .setEncoding(audioFormat)
                    .build(),
                minBufOut,
                AudioTrack.MODE_STREAM,
                AudioManager.AUDIO_SESSION_ID_GENERATE
            )

            var seq = 0
            val headerSize = 2 + 1 + 2 + 4 + 4 + 1

            // Enable audio effects if available
            try {
                if (AcousticEchoCanceler.isAvailable()) {
                    AcousticEchoCanceler.create(recorder.audioSessionId)?.enabled = true
                }
                if (NoiseSuppressor.isAvailable()) {
                    NoiseSuppressor.create(recorder.audioSessionId)?.enabled = true
                }
                if (AutomaticGainControl.isAvailable()) {
                    AutomaticGainControl.create(recorder.audioSessionId)?.enabled = true
                }
            } catch (_: Exception) {}

            val frameDurationMs = 10
            val samplesPerFrame = (sampleRate / 1000) * frameDurationMs

            try {
                // Network setup wrapped in try/catch to avoid crashes on invalid host/IP
                val serverAddr = InetAddress.getByName(serverIp)
                val socket = DatagramSocket()
                socket.reuseAddress = true

                // Receiver thread now starts after successful network setup
                recvThread = Thread {
                    val recvBuf = ByteArray(4096)
                    track.play()
                    // Start periodic decay updater on UI thread
                    runOnUiThread { window?.decorView?.removeCallbacks(decayRunnable); window?.decorView?.post(decayRunnable) }
                    var lastPresenceReqMs = 0L
                    while (running.get()) {
                        try {
                            val packet = DatagramPacket(recvBuf, recvBuf.size)
                            socket.receive(packet)
                            if (packet.length <= headerSize) continue
                            val codec = recvBuf[headerSize - 1].toInt() and 0xFF
                            val payload = recvBuf.copyOfRange(headerSize, packet.length)
                            if (payload.isEmpty()) continue
                            val kind = payload[0].toInt()
                            if (kind == 1) {
                                // Control JSON
                                val json = String(payload, 1, payload.size - 1, Charsets.UTF_8)
                                handleControlMessage(json)
                                val t = extractString(json, "t")
                                if (t == "ack") {
                                    presenceReceived = true
                                    updateStatus("Подключено")
                                } else if (t == "presence") {
                                    rosterReceived = true
                                    if (presenceContainsId(json, clientId)) {
                                        presenceReceived = true
                                        updateStatus("Подключено")
                                    }
                                } else if (t == "join") {
                                    updateStatus("Подключено")
                                }
                                continue
                            }
                            // Audio
                            if (payload.size <= 1 + 4) continue
                            val idBuf = ByteBuffer.wrap(payload, 1, 4).order(ByteOrder.BIG_ENDIAN)
                            val senderId = idBuf.int
                            // Drop our own frames to avoid echo if reflected
                            if (senderId == clientId) continue
                            val audioData = payload.copyOfRange(1 + 4, payload.size)
                            // Only PCM now
                            // Ensure unknown speaker is visible in roster immediately
                            var added = false
                            synchronized(participants) {
                                if (!participants.containsKey(senderId)) {
                                    participants[senderId] = Participant(senderId, "User" + abs(senderId % 1000))
                                    added = true
                                }
                            }
                            if (added) {
                                renderParticipants()
                                // Proactively ask roster to resolve names and stabilize list
                                val nowMs = System.currentTimeMillis()
                                if (nowMs - lastPresenceReqMs > 800) {
                                    lastPresenceReqMs = nowMs
                                    try {
                                val h2 = ByteBuffer.allocate(headerSize).order(ByteOrder.BIG_ENDIAN)
                                val who = """{"t":"presence_req","stream":$streamId}"""
                                val magicLocal: Short = 0xA11D.toShort()
                                val versionLocal = 1
                                sendControl(socket, serverAddr, serverPort, h2, magicLocal, versionLocal, streamId, seq, who)
                                    } catch (_: Exception) {}
                                }
                            }
                            track.write(audioData, 0, audioData.size)
                            // First audio from server also proves connectivity
                            if (!presenceReceived) {
                                presenceReceived = true
                                updateStatus("Подключено")
                            }
                            // Update speaking indicator only if audio has non-trivial RMS
                            val rmsLevel = try { computeRmsPercent(audioData, audioData.size) } catch (_: Exception) { 0 }
                            if (rmsLevel > 5) {
                                synchronized(speakingTimestamps) {
                                    speakingTimestamps[senderId] = System.currentTimeMillis()
                                }
                            }
                        } catch (_: Exception) {
                            if (!running.get()) break
                        }
                    }
                }
                recvThread?.start()

                val pcmBytes = ByteArray(samplesPerFrame * 2)
                val header = ByteBuffer.allocate(headerSize).order(ByteOrder.BIG_ENDIAN)
                val magic: Short = 0xA11D.toShort()
                val version: Byte = 1
                val codecPcm: Byte = 0
                // Send join control BEFORE recording to avoid potential block
                val joinJson = """{"t":"join","id":$clientId,"name":"${displayName.replace("\"","'")}","stream":$streamId}"""
                presenceReceived = false
                rosterReceived = false
                sendControl(socket, serverAddr, serverPort, header, magic, version.toInt(), streamId, seq++, joinJson)
                // Connectivity timeout watcher (3s) + retry join up to 3 times until ack/presence
                Thread {
                    var attempts = 0
                    while (running.get() && !presenceReceived && attempts < 3) {
                        try { Thread.sleep(1000) } catch (_: InterruptedException) {}
                        if (running.get() && !presenceReceived) {
                            attempts++
                            sendControl(socket, serverAddr, serverPort, header, magic, version.toInt(), streamId, seq++, joinJson)
                        }
                    }
                    if (running.get() && !presenceReceived) {
                        updateStatus("Нет ответа от сервера")
                    }
                }.start()
                // Proactively request roster if not received soon after join
                Thread {
                    try { Thread.sleep(1200) } catch (_: InterruptedException) {}
                    var reqs = 0
                    while (running.get() && !rosterReceived && reqs < 2) {
                        val who = """{"t":"presence_req","stream":$streamId}"""
                        sendControl(socket, serverAddr, serverPort, header, magic, version.toInt(), streamId, seq++, who)
                        reqs++
                        try { Thread.sleep(1000) } catch (_: InterruptedException) {}
                    }
                }.start()
                // Start audio after join is sent
                recorder.startRecording()
                setUiConnected(true)
                // add self to participants UI
                synchronized(participants) {
                    participants[clientId] = Participant(clientId, displayName, speaking = false)
                }
                renderParticipants()
                while (running.get()) {
                    val read = recorder.read(pcmBytes, 0, pcmBytes.size)
                    if (read <= 0) continue
                    // VAD: compute RMS for UI
                    val level = computeRmsPercent(pcmBytes, read)
                    updateMicLevel(level)
                    header.clear()
                    header.putShort(magic)
                    header.put(version)
                    header.putShort(streamId.toShort())
                    header.putInt(seq)
                    header.putInt((System.currentTimeMillis() % Int.MAX_VALUE).toInt())
                    header.put(codecPcm)
                    // payload: kind(1)=audio, clientId(4), audioData
                    val out = ByteArray(headerSize + 1 + 4 + read)
                    System.arraycopy(header.array(), 0, out, 0, headerSize)
                    out[headerSize] = 0 // kind audio
                    val idbb = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(clientId)
                    System.arraycopy(idbb.array(), 0, out, headerSize + 1, 4)
                    System.arraycopy(pcmBytes, 0, out, headerSize + 1 + 4, read)
                    val dp = DatagramPacket(out, out.size, serverAddr, serverPort)
                    socket.send(dp)
                    seq += 1
                }
            } catch (ex: Exception) {
                updateStatus("Ошибка подключения: ${ex.message ?: "unknown"}")
                // Ensure we rollback UI state if connection failed early
                setUiConnected(false)
                running.set(false)
            } finally {
                // Send leave a few times to improve reliability over UDP before closing socket
                try {
                    val leaveJson = """{"t":"leave","id":$clientId}"""
                    val h = ByteBuffer.allocate(headerSize).order(ByteOrder.BIG_ENDIAN)
                    val magic: Short = 0xA11D.toShort()
                    // Attempt multiple sends with small delays
                    for (i in 0 until 3) {
                        try {
                            val serverAddrSafe = try { InetAddress.getByName(serverIp) } catch (_: Exception) { null }
                            if (serverAddrSafe != null) {
                                sendControl(DatagramSocket().apply { reuseAddress = true }, serverAddrSafe, serverPort, h, magic, 1, streamId, 0, leaveJson)
                            }
                        } catch (_: Exception) { }
                        try { Thread.sleep(50) } catch (_: InterruptedException) {}
                    }
                } catch (_: Exception) {}
                try { recorder.stop() } catch (_: Exception) {}
                try { recorder.release() } catch (_: Exception) {}
                try { track.stop() } catch (_: Exception) {}
                try { track.release() } catch (_: Exception) {}
            }
        }
        ioThread?.start()
    }

    private fun stopStreaming() {
        running.set(false)
        ioThread?.join(500)
        ioThread = null
        recvThread?.join(500)
        recvThread = null
        setUiConnected(false)
        updateStatus("Отключено")
        participants.clear()
        renderParticipants()
    }

    private fun updateStatus(msg: String) {
        runOnUiThread {
            chipStatus.text = msg
            chipStatus.setChipIconResource(if (msg.contains("Подключ")) android.R.drawable.presence_online else android.R.drawable.presence_offline)
        }
    }

    private fun setUiConnected(connected: Boolean) {
        runOnUiThread {
            btnStart.isEnabled = !connected
            btnStop.isEnabled = connected
            editName.isEnabled = !connected
            editIp.isEnabled = !connected
            editPort.isEnabled = !connected
            editStream.isEnabled = !connected
        }
    }

    private fun computeRmsPercent(buf: ByteArray, len: Int): Int {
        var sum: Long = 0
        val samples = len / 2
        val bb = ByteBuffer.wrap(buf, 0, len).order(ByteOrder.LITTLE_ENDIAN)
        for (i in 0 until samples) {
            val s = bb.short.toInt()
            sum += (s * s).toLong()
        }
        val mean = if (samples > 0) sum / samples else 0
        val rms = Math.sqrt(mean.toDouble())
        val percent = min(100, (rms / 32768.0 * 400).toInt()) // scale aggressively
        return percent
    }

    private fun updateMicLevel(level: Int) {
        runOnUiThread { progressMic.progress = level }
    }

    private data class Participant(var id: Int, var name: String, var speaking: Boolean = false)
    private val participants = mutableMapOf<Int, Participant>()

    private fun handleControlMessage(json: String) {
        // Robust parsing: detect type via extractor (handles optional spaces)
        when (extractString(json, "t")) {
            "join" -> {
                val id = extractInt(json, "id")
                val name = extractString(json, "name")
                if (id != null) {
                    participants[id] = Participant(id, name ?: ("User" + abs(id % 1000)))
                    renderParticipants()
                }
            }
            "leave" -> {
                val id = extractInt(json, "id")
                if (id != null) {
                    participants.remove(id)
                    synchronized(speakingTimestamps) { speakingTimestamps.remove(id) }
                    renderParticipants()
                }
            }
            "presence" -> {
                // Extract members array content (allow newlines/spaces)
                val membersArray = Regex("\\\"members\\\"\\s*:\\s*\\[(.*?)]", setOf(RegexOption.DOT_MATCHES_ALL))
                    .find(json)?.groupValues?.getOrNull(1)
                val presenceIds = mutableSetOf<Int>()
                if (!membersArray.isNullOrBlank()) {
                    val itemRx = Regex("\\{[^\\\\{}]*}")
                    itemRx.findAll(membersArray).forEach { m ->
                        val item = m.value
                        val id = extractInt(item, "id") ?: return@forEach
                        val name = extractString(item, "name") ?: ("User" + abs(id % 1000))
                        presenceIds.add(id)
                        val existing = participants[id]
                        if (existing == null) {
                            participants[id] = Participant(id, name, speaking = false)
                        } else if (existing.name != name) {
                            existing.name = name
                        }
                    }
                }
                // Remove participants not present in presence and not recently speaking (keeps transient audio-only senders)
                val now = System.currentTimeMillis()
                val iterator = participants.iterator()
                while (iterator.hasNext()) {
                    val entry = iterator.next()
                    val id = entry.key
                    if (id == clientId) continue
                    if (!presenceIds.contains(id)) {
                        val lastSpeak = synchronized(speakingTimestamps) { speakingTimestamps[id] }
                        if (lastSpeak == null || now - lastSpeak > 5000) {
                            synchronized(speakingTimestamps) { speakingTimestamps.remove(id) }
                            iterator.remove()
                        }
                    }
                }
                renderParticipants()
                updateStatus("Подключено")
            }
        }
    }

    private fun updateSpeakingFromTimestamps(ts: Map<Int, Long>) {
        val now = System.currentTimeMillis()
        var changed = false
        for ((id, p) in participants) {
            val speakingNow = ts[id]?.let { now - it < 400 } == true
            if (p.speaking != speakingNow) {
                p.speaking = speakingNow
                changed = true
            }
        }
        if (changed) renderParticipants()
    }

    private fun renderParticipants() {
        runOnUiThread {
            participantsContainer.removeAllViews()
            participants.values.sortedBy { it.name.lowercase() }.forEach { p ->
                val tv = TextView(this)
                tv.textSize = 16f
                tv.text = (if (p.speaking) "● " else "○ ") + p.name
                tv.setTextColor(if (p.speaking) 0xFF2E7D32.toInt() else 0xFF666666.toInt())
                participantsContainer.addView(tv)
            }
            if (participants.isEmpty()) {
                val tv = TextView(this)
                tv.text = "Нет подключений"
                tv.setTextColor(0xFF888888.toInt())
                participantsContainer.addView(tv)
            }
        }
    }

    private fun extractInt(json: String, key: String): Int? {
        val rx = Regex("\\\"$key\\\"\\s*:\\s*(-?\\d+)")
        val m = rx.find(json) ?: return null
        return m.groupValues[1].toIntOrNull()
    }

    private fun extractString(json: String, key: String): String? {
        val rx = Regex("\\\"$key\\\"\\s*:\\s*\\\"([^\\\\\\\"]*)\\\"")
        val m = rx.find(json) ?: return null
        return m.groupValues[1]
    }

    private fun presenceContainsId(json: String, id: Int): Boolean {
        val rx = Regex("\\\"members\\\"\\s*:\\s*\\[(.*?)]", setOf(RegexOption.DOT_MATCHES_ALL))
        val membersArray = rx.find(json)?.groupValues?.getOrNull(1) ?: return false
        val idRx = Regex("\\\"id\\\"\\s*:\\s*$id")
        return idRx.containsMatchIn(membersArray)
    }

    private fun sendControl(
        socket: DatagramSocket,
        serverAddr: InetAddress,
        serverPort: Int,
        header: ByteBuffer,
        magic: Short,
        version: Int,
        streamId: Int,
        seq: Int,
        json: String
    ) {
        val headerSize = 2 + 1 + 2 + 4 + 4 + 1
        header.clear()
        header.putShort(magic)
        header.put(version.toByte())
        header.putShort(streamId.toShort())
        header.putInt(seq)
        header.putInt((System.currentTimeMillis() % Int.MAX_VALUE).toInt())
        header.put(0) // codec PCM placeholder
        val payload = json.toByteArray(Charsets.UTF_8)
        val out = ByteArray(headerSize + 1 + payload.size)
        System.arraycopy(header.array(), 0, out, 0, headerSize)
        out[headerSize] = 1 // kind control
        System.arraycopy(payload, 0, out, headerSize + 1, payload.size)
        val dp = DatagramPacket(out, out.size, serverAddr, serverPort)
        socket.send(dp)
    }
}


