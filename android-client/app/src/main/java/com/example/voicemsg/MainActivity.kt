package com.example.voicemsg

import android.Manifest
import android.content.ContentValues
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.drawable.ColorDrawable
import android.media.*
import android.media.audiofx.AcousticEchoCanceler
import android.media.audiofx.AutomaticGainControl
import android.media.audiofx.NoiseSuppressor
import android.net.Uri
import android.os.Bundle
import android.text.Editable
import android.text.TextWatcher
import android.view.View
import android.view.ViewTreeObserver
import android.view.inputmethod.InputMethodManager
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ProgressBar
import android.widget.TextView
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.core.content.FileProvider
import androidx.drawerlayout.widget.DrawerLayout
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.google.android.material.chip.Chip
import com.google.android.material.textfield.TextInputEditText
import androidx.core.net.toUri
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.abs
import kotlin.math.min
import kotlin.random.Random
import coil.load
import coil.decode.VideoFrameDecoder
import android.util.Base64
import java.io.File
import java.io.RandomAccessFile
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

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
    private lateinit var drawerLayout: DrawerLayout
    private lateinit var chatRecycler: RecyclerView
    private lateinit var editMessage: TextInputEditText
    private lateinit var btnAttach: android.widget.ImageButton
    private lateinit var btnSend: android.widget.ImageButton

    private val running = AtomicBoolean(false)
    private var ioThread: Thread? = null
    private var recvThread: Thread? = null
    private val controlLock = Any()
    @Volatile private var sendControlLambda: ((String) -> Unit)? = null
    @Volatile private var currentDisplayName: String = ""
    @Volatile private var currentStreamId: Int = 1

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

    // Track timestamps of our last sent chat messages to avoid duplicate echo
    private val recentSentChatTs: java.util.ArrayDeque<Long> = java.util.ArrayDeque()
    // Global dedup across reconnects: remember (fromId|ts)
    private val seenChatKeys: java.util.LinkedHashSet<String> = object : java.util.LinkedHashSet<String>() {
        override fun add(element: String): Boolean {
            val added = super.add(element)
            if (size > 2000) {
                // remove oldest ~100 to keep set bounded
                val iter = iterator()
                repeat(100) { if (iter.hasNext()) { iter.next(); iter.remove() } }
            }
            return added
        }
    }

    // Track which media (by mid) were already displayed to avoid duplicate inserts on retransmits/replays
    private val displayedMediaMids: java.util.LinkedHashSet<String> = object : java.util.LinkedHashSet<String>() {
        override fun add(element: String): Boolean {
            val added = super.add(element)
            if (size > 2000) {
                val iter = iterator()
                repeat(100) { if (iter.hasNext()) { iter.next(); iter.remove() } }
            }
            return added
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
        drawerLayout = findViewById(R.id.drawerLayout)
        chatRecycler = findViewById(R.id.chatRecycler)
        editMessage = findViewById(R.id.editMessage)
        btnAttach = findViewById(R.id.btnAttach)
        btnSend = findViewById(R.id.btnSend)

        setupChatUi()

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

    private fun setupChatUi() {
        chatRecycler.layoutManager = LinearLayoutManager(this).apply { stackFromEnd = true }
        chatRecycler.adapter = chatAdapter
        // small peek by setting scrim color and locking until drag
        drawerLayout.setScrimColor(0x22000000)
        drawerLayout.setDrawerElevation(8f)
        // disable programmatic open; rely on swipe from right
        drawerLayout.addDrawerListener(object : DrawerLayout.SimpleDrawerListener() {
            override fun onDrawerOpened(drawerView: View) {
                chatRecycler.scrollToPosition(chatAdapter.itemCount - 1)
            }
        })
        // Attach button shows chooser (gallery, camera photo, camera video)
        btnAttach.setOnClickListener { showAttachOptions() }
        btnSend.setOnClickListener {
            if (sendControlLambda == null) {
                // Not connected – avoid adding local-only messages
                try { android.widget.Toast.makeText(this, "Нет соединения с сервером", android.widget.Toast.LENGTH_SHORT).show() } catch (_: Exception) {}
                return@setOnClickListener
            }
            val text = editMessage.text?.toString()?.trim().orEmpty()
            if (text.isNotEmpty()) {
                val tsNow = System.currentTimeMillis()
                // Show with consistent prefix immediately
                chatAdapter.addMessage(ChatItem.Text("Вы: $text", tsNow, sender = "Вы", mine = true, status = MsgStatus.SENDING, ts = tsNow))
                // Remember ts to drop server echo duplicate shortly after
                try {
                    synchronized(recentSentChatTs) {
                        recentSentChatTs.add(tsNow)
                        while (recentSentChatTs.size > 50) recentSentChatTs.poll()
                    }
                    // Also mark as seen globally to avoid history replay duplicate after reconnect
                    synchronized(seenChatKeys) { seenChatKeys.add("${clientId}|${tsNow}") }
                } catch (_: Exception) {}
                // Broadcast to peers if connected
                // Send over UDP off the UI thread to avoid NetworkOnMainThreadException
                Thread {
                    try {
                        val json = """{"t":"chat","id":$clientId,"name":"${currentDisplayName.replace("\"","'")}","stream":$currentStreamId,"text":"${text.replace("\"","'")}","ts":$tsNow}"""
                        sendControlLambda?.invoke(json)
                    } catch (_: Exception) {}
                }.start()
                // Failure watchdog: if нет ack в разумный срок, помечаем как FAILED
                Thread {
                    try { Thread.sleep(7000) } catch (_: InterruptedException) {}
                    runOnUiThread { chatAdapter.setTextStatus(tsNow, MsgStatus.FAILED) }
                }.start()
                editMessage.setText("")
                chatRecycler.scrollToPosition(chatAdapter.itemCount - 1)
            }
        }
        // Ensure IME shows and supports Cyrillic input
        editMessage.setOnEditorActionListener { v, _, _ ->
            false
        }
    }

    private fun showAttachOptions() {
        // Simple system chooser via multiple contracts
        val pick = Intent(Intent.ACTION_OPEN_DOCUMENT).apply {
            addCategory(Intent.CATEGORY_OPENABLE)
            type = "*/*"
            putExtra(Intent.EXTRA_MIME_TYPES, arrayOf("image/*", "video/*"))
        }
        val cameraPhoto = Intent(android.provider.MediaStore.ACTION_IMAGE_CAPTURE)
        currentPhotoUri = createTempMediaUri(isVideo = false)
        cameraPhoto.putExtra(android.provider.MediaStore.EXTRA_OUTPUT, currentPhotoUri)
        val cameraVideo = Intent(android.provider.MediaStore.ACTION_VIDEO_CAPTURE).apply {
            putExtra(android.provider.MediaStore.EXTRA_DURATION_LIMIT, 60)
            putExtra(android.provider.MediaStore.EXTRA_SIZE_LIMIT, 200L * 1024L * 1024L)
        }
        currentVideoUri = createTempMediaUri(isVideo = true)
        cameraVideo.putExtra(android.provider.MediaStore.EXTRA_OUTPUT, currentVideoUri)

        val chooser = Intent.createChooser(pick, "Выберите файл")
        chooser.putExtra(Intent.EXTRA_INITIAL_INTENTS, arrayOf(cameraPhoto, cameraVideo))
        chooseContent.launch(chooser)
    }

    private var currentPhotoUri: Uri? = null
    private var currentVideoUri: Uri? = null

    private val chooseContent = registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { res ->
        val dataUri = res.data?.data
        val uri = dataUri ?: if (res.resultCode == RESULT_OK) currentPhotoUri ?: currentVideoUri else null
        if (uri != null) {
            // persist permission for SAF URIs
            try {
                val flags = res.data?.flags ?: 0
                contentResolver.takePersistableUriPermission(uri, flags and (Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_GRANT_WRITE_URI_PERMISSION))
            } catch (_: Exception) {}
            val mime = contentResolver.getType(uri) ?: ""
            if (mime.startsWith("image")) {
                val mid = "m" + java.util.UUID.randomUUID().toString().replace("-", "")
                chatAdapter.addMessage(ChatItem.Image(uri, System.currentTimeMillis(), mid = mid, progressPercent = 0, uploading = true))
                chatRecycler.scrollToPosition(chatAdapter.itemCount - 1)
                sendSelectedMedia(uri, mime, mid)
            } else if (mime.startsWith("video")) {
                val mid = "m" + java.util.UUID.randomUUID().toString().replace("-", "")
                chatAdapter.addMessage(ChatItem.Video(uri, System.currentTimeMillis(), mid = mid, progressPercent = 0, uploading = true))
                chatRecycler.scrollToPosition(chatAdapter.itemCount - 1)
                sendSelectedMedia(uri, mime, mid)
            }
        }
    }

    private fun sendSelectedMedia(uri: Uri, mime: String, midProvided: String) {
        // Read bytes and send via UDP as chunked base64 control packets
        Thread {
            try {
                // Determine file size without loading to RAM
                val isVideo = mime.startsWith("video")
                val mid = midProvided
                // Tune raw chunk to keep UDP datagram < ~1200-1250 bytes after base64+JSON
                val chunkRaw = 720 // bytes per chunk before base64
                var finalTotal = 0
                val sizeByQuery = try {
                    val cursor = contentResolver.query(uri, arrayOf(android.provider.OpenableColumns.SIZE), null, null, null)
                    val idxSz = cursor?.getColumnIndex(android.provider.OpenableColumns.SIZE) ?: -1
                    val v = if (idxSz >= 0 && cursor != null && cursor.moveToFirst()) cursor.getLong(idxSz) else -1L
                    cursor?.close()
                    v
                } catch (_: Exception) { -1L }
                val sizeByAfd = try { contentResolver.openAssetFileDescriptor(uri, "r")?.use { it.length } ?: -1L } catch (_: Exception) { -1L }
                val fileSize = if (sizeByQuery > 0) sizeByQuery else sizeByAfd
                if (fileSize <= 0) {
                    // Fallback: try to estimate by reading stream once (no buffering)
                    var measured = 0L
                    try {
                        contentResolver.openInputStream(uri)?.use { inp ->
                            val buf = ByteArray(64 * 1024)
                            while (true) {
                                val r = inp.read(buf)
                                if (r <= 0) break
                                measured += r
                            }
                        }
                    } catch (_: Exception) {}
                    if (measured <= 0) return@Thread
                    // total known now; we'll reopen stream below for actual send
                    val total = ((measured + chunkRaw - 1) / chunkRaw).toInt()
                    finalTotal = total
                    outgoingMedia[mid] = OutgoingMedia(mid, if (isVideo) "video" else "image", mime, chunkRaw, total, uri, measured)
                    var idx = 0
                    contentResolver.openInputStream(uri)?.use { inp ->
                        val buf = ByteArray(chunkRaw)
                        while (idx < total) {
                            var read = 0
                            while (read < buf.size) {
                                val r = inp.read(buf, read, buf.size - read)
                                if (r <= 0) break
                                read += r
                            }
                            if (read <= 0) break
                            val data = if (read == buf.size) buf else buf.copyOf(read)
                            val enc = Base64.encodeToString(data, Base64.NO_WRAP)
                            val json = """{"t":"media","id":$clientId,"name":"${currentDisplayName.replace("\"","'")}","stream":$currentStreamId,"mid":"$mid","mtype":"${if (isVideo) "video" else "image"}","mime":"$mime","cr":$chunkRaw,"index":$idx,"total":$total,"data":"$enc"}"""
                            sendControlLambda?.invoke(json)
                            idx += 1
                            val p = ((idx * 100.0) / total).toInt().coerceIn(0, 100)
                            runOnUiThread { chatAdapter.setMediaProgress(mid, p, done = false) }
                        }
                    }
                } else {
                    val total = ((fileSize + chunkRaw - 1) / chunkRaw).toInt()
                    finalTotal = total
                    outgoingMedia[mid] = OutgoingMedia(mid, if (isVideo) "video" else "image", mime, chunkRaw, total, uri, fileSize)
                    var idx = 0
                    contentResolver.openInputStream(uri)?.use { inp ->
                        val buf = ByteArray(chunkRaw)
                        while (idx < total) {
                            var read = 0
                            while (read < buf.size) {
                                val r = inp.read(buf, read, buf.size - read)
                                if (r <= 0) break
                                read += r
                            }
                            if (read <= 0) break
                            val data = if (read == buf.size) buf else buf.copyOf(read)
                            val enc = Base64.encodeToString(data, Base64.NO_WRAP)
                            val json = """{"t":"media","id":$clientId,"name":"${currentDisplayName.replace("\"","'")}","stream":$currentStreamId,"mid":"$mid","mtype":"${if (isVideo) "video" else "image"}","mime":"$mime","cr":$chunkRaw,"index":$idx,"total":$total,"data":"$enc"}"""
                            sendControlLambda?.invoke(json)
                            if (idx % 200 == 0) { try { Thread.sleep(1) } catch (_: InterruptedException) {} }
                            idx += 1
                            val p = ((idx * 100.0) / total).toInt().coerceIn(0, 100)
                            runOnUiThread { chatAdapter.setMediaProgress(mid, p, done = false) }
                        }
                    }
                }
                // Signal end so receivers can request missing chunks (include type + mime for late joiners)
                val endJson = """{"t":"media_end","mid":"$mid","total":$finalTotal,"mtype":"${if (isVideo) "video" else "image"}","mime":"$mime","cr":$chunkRaw,"id":$clientId,"stream":$currentStreamId}"""
                // Send завершающий сигнал более агрессивно, чтобы получатель точно узнал размер и запросил недостающие части
                repeat(6) { sendControlLambda?.invoke(endJson); try { Thread.sleep(5) } catch (_: InterruptedException) {} }
                runOnUiThread { chatAdapter.setMediaProgress(mid, 100, done = true) }
                // Mark as failed if не получим подтверждение доставки в разумный срок
                // Mark as failed only if мы не получили media_delivered от получателя в разумный срок
                Thread {
                    try { Thread.sleep(15000) } catch (_: InterruptedException) {}
                    // If already displayed on our side (i.e., receiver sent delivered), skip marking failed
                    synchronized(displayedMediaMids) {
                        if (!displayedMediaMids.contains(mid)) {
                            runOnUiThread { chatAdapter.setMediaFailed(mid) }
                        }
                    }
                }.start()
            } catch (_: Exception) {}
        }.start()
    }

    private fun createTempMediaUri(isVideo: Boolean): Uri? {
        return try {
            val time = SimpleDateFormat("yyyyMMdd_HHmmss", Locale.US).format(Date())
            val fileName = (if (isVideo) "VID_" else "IMG_") + time
            val values = ContentValues().apply {
                put(android.provider.MediaStore.MediaColumns.DISPLAY_NAME, fileName)
                put(android.provider.MediaStore.MediaColumns.MIME_TYPE, if (isVideo) "video/mp4" else "image/jpeg")
                put(android.provider.MediaStore.MediaColumns.RELATIVE_PATH, "DCIM/VoiceMsg")
            }
            val collection = if (isVideo) android.provider.MediaStore.Video.Media.EXTERNAL_CONTENT_URI else android.provider.MediaStore.Images.Media.EXTERNAL_CONTENT_URI
            contentResolver.insert(collection, values)
        } catch (_: Exception) { null }
    }

    private enum class MsgStatus { SENDING, SENT, DELIVERED, READ, FAILED }

    private sealed class ChatItem(val timeMs: Long) {
        class Text(
            val text: String,
            timeMs: Long,
            val sender: String,
            val mine: Boolean,
            var status: MsgStatus = if (mine) MsgStatus.SENDING else MsgStatus.DELIVERED,
            val ts: Long = timeMs
        ) : ChatItem(timeMs)
        class Image(
            val uri: Uri,
            timeMs: Long,
            val mid: String? = null,
            var progressPercent: Int = 0,
            var uploading: Boolean = false,
            var delivered: Boolean = false,
            var failed: Boolean = false
        ) : ChatItem(timeMs)
        class Video(
            val uri: Uri,
            timeMs: Long,
            val mid: String? = null,
            var progressPercent: Int = 0,
            var uploading: Boolean = false,
            var delivered: Boolean = false,
            var failed: Boolean = false
        ) : ChatItem(timeMs)
    }

    // Cache of our own outgoing media to support retransmit on request
    private data class OutgoingMedia(
        val mid: String,
        val mtype: String,
        val mime: String,
        val chunkRaw: Int,
        val total: Int,
        val uri: Uri,
        val fileSize: Long
    )
    private val outgoingMedia = java.util.concurrent.ConcurrentHashMap<String, OutgoingMedia>()

    private val chatAdapter = object : RecyclerView.Adapter<RecyclerView.ViewHolder>() {
        private val items = mutableListOf<ChatItem>()
        private val midToPosition = mutableMapOf<String, Int>()
        private val textTsToPosition = mutableMapOf<Long, Int>()

        fun addMessage(item: ChatItem) {
            items.add(item)
            notifyItemInserted(items.size - 1)
            val pos = items.size - 1
            when (item) {
                is ChatItem.Image -> item.mid?.let { midToPosition[it] = pos }
                is ChatItem.Video -> item.mid?.let { midToPosition[it] = pos }
                is ChatItem.Text -> if (item.mine) textTsToPosition[item.ts] = pos
                else -> {}
            }
        }

        override fun getItemCount(): Int = items.size
        override fun getItemViewType(position: Int): Int = when (items[position]) {
            is ChatItem.Text -> 0
            is ChatItem.Image -> 1
            is ChatItem.Video -> 2
        }

        override fun onCreateViewHolder(parent: android.view.ViewGroup, viewType: Int): RecyclerView.ViewHolder {
            return when (viewType) {
                0 -> TextVH(TextView(parent.context).apply {
                    textSize = 15f
                    setPadding(16, 10, 16, 10)
                })
                1 -> ImageVH(createMediaContainer(parent.context))
                else -> VideoVH(createMediaContainer(parent.context))
            }
        }

        override fun onBindViewHolder(holder: RecyclerView.ViewHolder, position: Int) {
            when (val item = items[position]) {
                is ChatItem.Text -> (holder as TextVH).tv.text = formatTextWithStatus(item)
                is ChatItem.Image -> (holder as ImageVH).bind(item)
                is ChatItem.Video -> (holder as VideoVH).bind(item)
            }
        }

        inner class TextVH(val tv: TextView) : RecyclerView.ViewHolder(tv)
        inner class ImageVH(private val container: android.widget.FrameLayout) : RecyclerView.ViewHolder(container) {
            private val iv: android.widget.ImageView = container.getChildAt(0) as android.widget.ImageView
            private val overlay: android.view.View = container.getChildAt(1)
            private val percentTv: TextView = (overlay as android.widget.LinearLayout).getChildAt(1) as TextView
            fun bind(item: ChatItem.Image) {
                iv.load(item.uri)
                if (item.uploading) {
                    overlay.visibility = View.VISIBLE
                    percentTv.text = "${item.progressPercent}%"
                    percentTv.setTextColor(0xFFFFFFFF.toInt())
                } else if (item.failed) {
                    overlay.visibility = View.VISIBLE
                    percentTv.text = "×"
                    percentTv.setTextColor(0xFFFF4444.toInt())
                } else {
                    overlay.visibility = View.GONE
                }
            }
        }
        inner class VideoVH(private val container: android.widget.FrameLayout) : RecyclerView.ViewHolder(container) {
            private val iv: android.widget.ImageView = container.getChildAt(0) as android.widget.ImageView
            private val overlay: android.view.View = container.getChildAt(1)
            private val percentTv: TextView = (overlay as android.widget.LinearLayout).getChildAt(1) as TextView
            fun bind(item: ChatItem.Video) {
                iv.load(item.uri) { decoderFactory(VideoFrameDecoder.Factory()) }
                if (item.uploading) {
                    overlay.visibility = View.VISIBLE
                    percentTv.text = "${item.progressPercent}%"
                    percentTv.setTextColor(0xFFFFFFFF.toInt())
                } else if (item.failed) {
                    overlay.visibility = View.VISIBLE
                    percentTv.text = "×"
                    percentTv.setTextColor(0xFFFF4444.toInt())
                } else {
                    overlay.visibility = View.GONE
                }
            }
        }

        private fun createMediaContainer(ctx: android.content.Context): android.widget.FrameLayout {
            val frame = android.widget.FrameLayout(ctx)
            frame.layoutParams = RecyclerView.LayoutParams(RecyclerView.LayoutParams.MATCH_PARENT, RecyclerView.LayoutParams.WRAP_CONTENT)
            val iv = android.widget.ImageView(ctx)
            iv.adjustViewBounds = true
            iv.layoutParams = android.widget.FrameLayout.LayoutParams(android.widget.FrameLayout.LayoutParams.MATCH_PARENT, android.widget.FrameLayout.LayoutParams.WRAP_CONTENT)
            frame.addView(iv)
            // overlay with spinner + percent text
            val overlay = android.widget.LinearLayout(ctx)
            overlay.orientation = android.widget.LinearLayout.VERTICAL
            overlay.gravity = android.view.Gravity.CENTER
            overlay.setBackgroundColor(0x33000000)
            val lp = android.widget.FrameLayout.LayoutParams(android.widget.FrameLayout.LayoutParams.MATCH_PARENT, android.widget.FrameLayout.LayoutParams.MATCH_PARENT)
            overlay.layoutParams = lp
            val spinner = ProgressBar(ctx)
            spinner.isIndeterminate = true
            val percent = TextView(ctx)
            percent.textSize = 16f
            percent.setTextColor(0xFFFFFFFF.toInt())
            overlay.addView(spinner)
            overlay.addView(percent)
            overlay.visibility = View.GONE
            frame.addView(overlay)
            return frame
        }

        fun setMediaProgress(mid: String, progress: Int, done: Boolean) {
            val pos = midToPosition[mid] ?: return
            val item = items.getOrNull(pos) ?: return
            when (item) {
                is ChatItem.Image -> {
                    item.progressPercent = progress
                    item.uploading = !done
                }
                is ChatItem.Video -> {
                    item.progressPercent = progress
                    item.uploading = !done
                }
                else -> {}
            }
            notifyItemChanged(pos)
            if (done) {
                midToPosition.remove(mid)
            }
        }

        fun setMediaFailed(mid: String) {
            val pos = midToPosition[mid] ?: return
            val item = items.getOrNull(pos) ?: return
            when (item) {
                is ChatItem.Image -> { item.uploading = false; item.failed = true }
                is ChatItem.Video -> { item.uploading = false; item.failed = true }
                else -> {}
            }
            notifyItemChanged(pos)
        }

        fun setTextStatus(ts: Long, status: MsgStatus) {
            val pos = textTsToPosition[ts] ?: return
            val item = items.getOrNull(pos) as? ChatItem.Text ?: return
            // Avoid downgrading status: FAILED only if still SENDING; READ highest
            val current = item.status
            val next = when (status) {
                MsgStatus.FAILED -> if (current == MsgStatus.SENDING) MsgStatus.FAILED else current
                MsgStatus.SENT -> if (current == MsgStatus.SENDING) MsgStatus.SENT else current
                MsgStatus.DELIVERED -> if (current == MsgStatus.SENDING || current == MsgStatus.SENT) MsgStatus.DELIVERED else current
                MsgStatus.READ -> MsgStatus.READ
                MsgStatus.SENDING -> current
            }
            item.status = next
            notifyItemChanged(pos)
            if (next == MsgStatus.FAILED || next == MsgStatus.READ) {
                textTsToPosition.remove(ts)
            }
        }

        private fun formatTextWithStatus(item: ChatItem.Text): String {
            if (!item.mine) return item.text
            val suffix = when (item.status) {
                MsgStatus.SENDING -> ""
                MsgStatus.SENT -> " ✓"
                MsgStatus.DELIVERED -> " ✓✓"
                MsgStatus.READ -> " ✓✓" // visually same; could be tinted in UI theme later
                MsgStatus.FAILED -> " ✕"
            }
            return item.text + suffix
        }
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
        currentDisplayName = displayName
        currentStreamId = streamId
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
                try { socket.receiveBufferSize = 1 shl 20 } catch (_: Exception) {}
                try { socket.sendBufferSize = 1 shl 20 } catch (_: Exception) {}

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
                val ctrlHeader = ByteBuffer.allocate(headerSize).order(ByteOrder.BIG_ENDIAN)
                val magic: Short = 0xA11D.toShort()
                val version: Byte = 1
                val codecPcm: Byte = 0
                // Send join control BEFORE recording to avoid potential block
                val joinJson = """{"t":"join","id":$clientId,"name":"${displayName.replace("\"","'")}","stream":$streamId}"""
                presenceReceived = false
                rosterReceived = false
                sendControl(socket, serverAddr, serverPort, ctrlHeader, magic, version.toInt(), streamId, seq++, joinJson)
                // Expose control sender for UI thread
                val seqRef = java.util.concurrent.atomic.AtomicInteger(seq)
                sendControlLambda = { json ->
                    try {
                        val s = seqRef.getAndIncrement()
                        sendControl(socket, serverAddr, serverPort, ctrlHeader, magic, version.toInt(), streamId, s, json)
                    } catch (_: Exception) { }
                }
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
                sendControlLambda = null
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
        // Do NOT clear seenChatKeys to avoid duplicate replay after quick reconnect
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
            // Chat controls available only when connected
            try {
                btnSend.isEnabled = connected
                btnAttach.isEnabled = connected
                editMessage.isEnabled = connected
            } catch (_: Exception) { }
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
            "chat" -> {
                val fromId = extractInt(json, "id")
                val name = extractString(json, "name") ?: fromId?.let { "User" + abs(it % 1000) } ?: "?"
                val text = extractString(json, "text") ?: return
                // Parse timestamp as Long to avoid overflow and dedup correctly after reconnects
                val ts = extractLong(json, "ts")
                val mine = fromId == clientId
                val senderLabel = if (mine) "Вы" else name
                // Drop duplicate echoes of our own message if we already added it locally
                if (mine && ts != null) {
                    val shouldDrop = try {
                        synchronized(recentSentChatTs) {
                            val it = recentSentChatTs.iterator()
                            var found = false
                            while (it.hasNext()) {
                                val v = it.next()
                                if (v == ts) { found = true; it.remove(); break }
                            }
                            found
                        }
                    } catch (_: Exception) { false }
                    if (shouldDrop) return
                }
                val msgTs = ts ?: System.currentTimeMillis()
                // Global dedup: if we've seen (id|ts) already (e.g., after reconnect history replay), skip
                val key = (fromId?.toString() ?: "?") + "|" + msgTs.toString()
                synchronized(seenChatKeys) {
                    if (seenChatKeys.contains(key)) return
                    seenChatKeys.add(key)
                }
                runOnUiThread {
                    chatAdapter.addMessage(ChatItem.Text("$senderLabel: $text", msgTs, sender = senderLabel, mine = mine, status = if (mine) MsgStatus.SENT else MsgStatus.DELIVERED, ts = msgTs))
                    chatRecycler.scrollToPosition(chatAdapter.itemCount - 1)
                }
                // Send delivery/read receipts for incoming messages
                if (!mine && ts != null) {
                    val delivered = """{"t":"chat_delivered","orig_id":${fromId ?: -1},"ts":$ts,"id":$clientId,"stream":$currentStreamId}"""
                    val read = """{"t":"chat_read","orig_id":${fromId ?: -1},"ts":$ts,"id":$clientId,"stream":$currentStreamId}"""
                    Thread { try { sendControlLambda?.invoke(delivered); Thread.sleep(20); sendControlLambda?.invoke(read) } catch (_: Exception) {} }.start()
                }
            }
            "chat_ack" -> {
                val ts = extractLong(json, "ts") ?: return
                runOnUiThread { chatAdapter.setTextStatus(ts, MsgStatus.SENT) }
            }
            "chat_delivered" -> {
                val ts = extractLong(json, "ts") ?: return
                runOnUiThread { chatAdapter.setTextStatus(ts, MsgStatus.DELIVERED) }
            }
            "chat_read" -> {
                val ts = extractLong(json, "ts") ?: return
                runOnUiThread { chatAdapter.setTextStatus(ts, MsgStatus.READ) }
            }
            "media" -> {
                val mid = extractString(json, "mid") ?: return
                val mtype = extractString(json, "mtype") ?: return
                val mime = extractString(json, "mime") ?: return
                val cr = extractInt(json, "cr") ?: 900
                val name = extractString(json, "name") ?: "?"
                val index = extractInt(json, "index") ?: return
                val total = extractInt(json, "total") ?: return
                val dataB64 = extractString(json, "data") ?: return
                val fromId = extractInt(json, "id")
                // Skip processing if we already displayed this media
                synchronized(displayedMediaMids) { if (displayedMediaMids.contains(mid)) return }
                handleIncomingMediaChunk(mid, mtype, mime, name, index, total, dataB64, fromId, cr)
            }
            "media_end" -> {
                val mid = extractString(json, "mid") ?: return
                val total = extractInt(json, "total") ?: return
                val cr = extractInt(json, "cr") ?: 900
                // If already displayed, ignore history replays/duplicates
                synchronized(displayedMediaMids) { if (displayedMediaMids.contains(mid)) return }
                var acc = incomingMedia[mid]
                if (acc == null) {
                    // Мы вообще не получили ни одного чанка — создаём пустой аккумулятор, чтобы запросить все индексы
                    val mtype = extractString(json, "mtype") ?: "image"
                    val mime = extractString(json, "mime") ?: "image/jpeg"
                    acc = MediaAccumulator(mtype = mtype, mime = mime, total = total, received = BooleanArray(total), chunkRaw = cr)
                    incomingMedia[mid] = acc
                }
                // If some chunks missing, request retransmit
                val missing = mutableListOf<Int>()
                for (i in 0 until total) if (!(acc.received.getOrNull(i) ?: false)) missing.add(i)
                if (missing.isNotEmpty()) {
                    // Request aggressively with smaller delay and moderate batch size to reduce packet size
                    sendMediaReqBatches(mid, missing, repeats = 6, delayMs = 60, batchSize = 120)
                }
            }
            "media_delivered" -> {
                val mid = extractString(json, "mid") ?: return
                // mark delivered -> no red cross
                runOnUiThread { /* no-op: could add a small checkmark overlay */ }
            }
            "media_req" -> {
                val mid = extractString(json, "mid") ?: return
                val needArray = Regex("\\\"need\\\"\\s*:\\s*\\[(.*?)]", setOf(RegexOption.DOT_MATCHES_ALL)).find(json)?.groupValues?.getOrNull(1) ?: return
                val idxRx = Regex("-?\\d+")
                val indexes = idxRx.findAll(needArray).mapNotNull { it.value.toIntOrNull() }.toList()
                val og = outgoingMedia[mid] ?: return
                for (i in indexes) {
                    if (i < 0 || i >= og.total) continue
                    val start = i * og.chunkRaw
                    val end = kotlin.math.min(start + og.chunkRaw, og.fileSize.toInt())
                    val bytes = contentResolver.openInputStream(og.uri)?.use { input ->
                        // Skip to start
                        if (start > 0) input.skip(start.toLong())
                        val buf = ByteArray(end - start)
                        var read = 0
                        while (read < buf.size) {
                            val r = input.read(buf, read, buf.size - read)
                            if (r <= 0) break
                            read += r
                        }
                        if (read < buf.size) buf.copyOf(read) else buf
                    } ?: continue
                    val enc = Base64.encodeToString(bytes, Base64.NO_WRAP)
                    val jsonOut = """{"t":"media","id":$clientId,"name":"${currentDisplayName.replace("\"","'")}","stream":$currentStreamId,"mid":"$mid","mtype":"${og.mtype}","mime":"${og.mime}","cr":${og.chunkRaw},"index":$i,"total":${og.total},"data":"$enc"}"""
                    sendControlLambda?.invoke(jsonOut)
                    try { Thread.sleep(1) } catch (_: InterruptedException) {}
                }
            }
        }
    }

    private data class MediaAccumulator(
        val mtype: String,
        val mime: String,
        val total: Int,
        val received: BooleanArray,
        var fromId: Int? = null,
        val chunkRaw: Int = 900
    )

    private data class PartialFile(
        val tmpFile: File,
        val raf: RandomAccessFile,
        val total: Int,
        val chunkRaw: Int,
        val received: BooleanArray
    )

    private val incomingMedia = mutableMapOf<String, MediaAccumulator>()
    private val incomingFiles = mutableMapOf<String, PartialFile>()
    // Track which mids already have a scheduled watchdog to request missing chunks
    private val mediaWatchdogScheduled = mutableSetOf<String>()

    private fun handleIncomingMediaChunk(
        mid: String,
        mtype: String,
        mime: String,
        name: String,
        index: Int,
        total: Int,
        dataB64: String,
        fromId: Int?,
        chunkRawFromMsg: Int
    ) {
        // If already displayed, ignore further chunks for this mid
        synchronized(displayedMediaMids) { if (displayedMediaMids.contains(mid)) return }
        var acc = incomingMedia[mid]
        if (acc == null) {
            acc = MediaAccumulator(mtype = mtype, mime = mime, total = total, received = BooleanArray(total), fromId = fromId, chunkRaw = chunkRawFromMsg)
            incomingMedia[mid] = acc
            // Create partial file holder
            try {
                val ext = when {
                    mime.contains("jpeg") -> "jpg"
                    mime.contains("png") -> "png"
                    mime.contains("gif") -> "gif"
                    mime.contains("webp") -> "webp"
                    mime.startsWith("video/") -> "mp4"
                    else -> "bin"
                }
                val tmp = File(cacheDir, "chat_${mid}.part.$ext")
                val raf = RandomAccessFile(tmp, "rw")
                // Pre-allocate approximate size to reduce fragmentation (best-effort)
                try { raf.setLength(total.toLong() * acc.chunkRaw) } catch (_: Exception) {}
                incomingFiles[mid] = PartialFile(tmp, raf, total, acc.chunkRaw, acc.received)
            } catch (_: Exception) {}
            // Schedule a watchdog to request missing chunks even if media_end is lost
            if (!mediaWatchdogScheduled.contains(mid)) {
                mediaWatchdogScheduled.add(mid)
                Thread {
                    try { Thread.sleep(900) } catch (_: InterruptedException) {}
                    val a = incomingMedia[mid]
                    if (a != null) {
                        val missing = mutableListOf<Int>()
                        for (i in 0 until a.total) if (!(a.received.getOrNull(i) ?: false)) missing.add(i)
                        if (missing.isNotEmpty()) {
                            // Faster watchdog retry with tighter delay
                            sendMediaReqBatches(mid, missing, repeats = 4, delayMs = 10, batchSize = 120)
                        }
                    }
                    mediaWatchdogScheduled.remove(mid)
                }.start()
            }
        }
        if (acc.fromId == null && fromId != null) acc.fromId = fromId
        // Write chunk directly to file
        val pf = incomingFiles[mid]
        try {
            if (pf != null && index in 0 until acc.total && !acc.received[index]) {
                val data = Base64.decode(dataB64, Base64.NO_WRAP)
                val offset = index.toLong() * acc.chunkRaw
                pf.raf.seek(offset)
                pf.raf.write(data)
                acc.received[index] = true
            }
        } catch (_: Exception) {}
        // If all received, finalize
        if (acc.received.all { it }) {
            incomingMedia.remove(mid)
            val holder = incomingFiles.remove(mid)
            mediaWatchdogScheduled.remove(mid)
            Thread {
                try {
                    holder?.raf?.channel?.force(true)
                } catch (_: Exception) {}
                try { holder?.raf?.close() } catch (_: Exception) {}
                try {
                    val finalFile = try {
                        val ext = when {
                            acc.mime.contains("jpeg") -> "jpg"
                            acc.mime.contains("png") -> "png"
                            acc.mime.contains("gif") -> "gif"
                            acc.mime.contains("webp") -> "webp"
                            acc.mime.startsWith("video/") -> "mp4"
                            else -> "bin"
                        }
                        File(cacheDir, "chat_$mid.$ext")
                    } catch (_: Exception) { null }
                    if (holder != null && finalFile != null) {
                        try { holder.tmpFile.renameTo(finalFile) } catch (_: Exception) {}
                        val uri = finalFile.toUri()
                        synchronized(displayedMediaMids) { displayedMediaMids.add(mid) }
                        runOnUiThread {
                            if (acc.mtype == "image") {
                                chatAdapter.addMessage(ChatItem.Image(uri, System.currentTimeMillis(), mid = mid))
                            } else {
                                chatAdapter.addMessage(ChatItem.Video(uri, System.currentTimeMillis(), mid = mid))
                            }
                            chatRecycler.scrollToPosition(chatAdapter.itemCount - 1)
                        }
                        val orig = acc.fromId
                        if (orig != null) {
                            val delivered = """{"t":"media_delivered","mid":"$mid","orig_id":$orig,"id":$clientId,"stream":$currentStreamId}"""
                            try { sendControlLambda?.invoke(delivered) } catch (_: Exception) {}
                        }
                    }
                } catch (_: Exception) {}
            }.start()
        }
    }

    private fun sendMediaReqBatches(mid: String, missing: List<Int>, repeats: Int, delayMs: Long, batchSize: Int) {
        val grouped = mutableListOf<List<Int>>()
        var i = 0
        while (i < missing.size) {
            val j = kotlin.math.min(i + batchSize, missing.size)
            grouped.add(missing.subList(i, j))
            i = j
        }
        Thread {
            repeat(repeats) {
                for (chunk in grouped) {
                    val need = chunk.joinToString(prefix = "[", postfix = "]")
                    val req = """{"t":"media_req","mid":"$mid","need":$need,"id":$clientId,"stream":$currentStreamId}"""
                    sendControlLambda?.invoke(req)
                    try { Thread.sleep(5) } catch (_: InterruptedException) {}
                }
                try { Thread.sleep(delayMs) } catch (_: InterruptedException) {}
            }
        }.start()
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

    private fun extractLong(json: String, key: String): Long? {
        val rx = Regex("\\\"$key\\\"\\s*:\\s*(-?\\d+)")
        val m = rx.find(json) ?: return null
        return try { m.groupValues[1].toLong() } catch (_: Exception) { null }
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


