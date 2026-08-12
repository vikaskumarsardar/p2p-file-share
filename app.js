// -----------------------------------------------------------------------------
// FlashShare P2P - WebRTC RTCDataChannel Direct Multi-File Transfer Engine
// Supports Dual Mode: 1) Automated Signaling Server 2) 100% Serverless Offline
// Supports Full-Duplex Simultaneous Send & Receive
// Supports Selective File Removal, Skip Active File, & Cancel Batch Controls
// -----------------------------------------------------------------------------

const CHUNK_SIZE = 64 * 1024; // 64 KB binary chunks
const rtcConfig = {
  iceServers: [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun1.l.google.com:19302" }
  ]
};

// Global State
let socket;
let currentRoom = "";
let peerConnections = new Map(); // peerId -> RTCPeerConnection
let dataChannels = new Map();    // peerId -> RTCDataChannel
let selectedFiles = [];          // Current pending batch of File objects
let outgoingQueue = [];          // Active transfer queue
let isSending = false;           // Sending state flag
let cancelSendRequested = false; // Batch cancellation flag
let skipFileRequested = false;   // Selective single file skip flag
let myPeerId = "";
let serverlessPeerConnection = null;

// Metrics State - Independent Full-Duplex Trackers
let sendStartTime = 0;
let sendBytesTransferred = 0;
let sendSpeedInterval = null;

let receiveStartTime = 0;
let receiveBytesTransferred = 0;
let receiveSpeedInterval = null;

// Receiver State
let incomingMetadata = null;
let receivedChunks = [];
let receivedBytes = 0;

// DOM Elements - Mode Switching
const tabSignaled = document.getElementById("tab-signaled");
const tabServerless = document.getElementById("tab-serverless");
const panelSignaled = document.getElementById("panel-signaled");
const panelServerless = document.getElementById("panel-serverless");

// DOM Elements - Signaled Mode
const connectionStatus = document.getElementById("connection-status");
const statusText = document.getElementById("status-text");
const roomIdInput = document.getElementById("room-id");
const btnJoin = document.getElementById("btn-join");
const peerCount = document.getElementById("peer-count");
const peerList = document.getElementById("peer-list");

// DOM Elements - Serverless Mode
const btnCreateOffer = document.getElementById("btn-create-offer");
const localOfferText = document.getElementById("local-offer-text");
const btnCopyOffer = document.getElementById("btn-copy-offer");
const remoteOfferText = document.getElementById("remote-offer-text");
const btnCreateAnswer = document.getElementById("btn-create-answer");
const localAnswerText = document.getElementById("local-answer-text");
const btnCopyAnswer = document.getElementById("btn-copy-answer");
const remoteAnswerText = document.getElementById("remote-answer-text");
const btnConnectAnswer = document.getElementById("btn-connect-answer");

// DOM Elements - File Selection
const dropZone = document.getElementById("drop-zone");
const fileInput = document.getElementById("file-input");
const fileDetails = document.getElementById("file-details");
const batchCount = document.getElementById("batch-count");
const fileSizeEl = document.getElementById("file-size");
const selectedFileList = document.getElementById("selected-file-list");
const btnSend = document.getElementById("btn-send");

// DOM Elements - Outgoing Send Dashboard
const sendDashboard = document.getElementById("send-dashboard");
const sendTitle = document.getElementById("send-title");
const sendPercent = document.getElementById("send-percent");
const sendProgressBar = document.getElementById("send-progress-bar");
const sendSpeed = document.getElementById("send-speed");
const sendChunks = document.getElementById("send-chunks");
const btnSkipFile = document.getElementById("btn-skip-file");
const btnCancelSend = document.getElementById("btn-cancel-send");
const metricRtt = document.getElementById("metric-rtt");

// DOM Elements - Incoming Receive Dashboard
const receiveDashboard = document.getElementById("receive-dashboard");
const receiveTitle = document.getElementById("receive-title");
const receivePercent = document.getElementById("receive-percent");
const receiveProgressBar = document.getElementById("receive-progress-bar");
const receiveSpeed = document.getElementById("receive-speed");
const receiveChunks = document.getElementById("receive-chunks");
const receiveStatus = document.getElementById("receive-status");
const btnCancelReceive = document.getElementById("btn-cancel-receive");
const downloadArea = document.getElementById("download-area");
const downloadsList = document.getElementById("downloads-list");

// -----------------------------------------------------------------------------
// 1. Mode Switching Handlers
// -----------------------------------------------------------------------------
tabSignaled.addEventListener("click", () => {
  tabSignaled.classList.add("active");
  tabServerless.classList.remove("active");
  panelSignaled.classList.remove("hidden");
  panelServerless.classList.add("hidden");
});

tabServerless.addEventListener("click", () => {
  tabServerless.classList.add("active");
  tabSignaled.classList.remove("active");
  panelServerless.classList.remove("hidden");
  panelSignaled.classList.add("hidden");
});

// -----------------------------------------------------------------------------
// 2. Socket.IO Signaling Setup (Mode 1)
// -----------------------------------------------------------------------------
function initSignaling() {
  if (typeof io === "undefined") {
    console.log("[Offline Standalone] Socket.IO library not loaded. Running in 100% Serverless Mode.");
    statusText.textContent = "Serverless Standalone";
    return;
  }
  socket = io();

  socket.on("connect", () => {
    myPeerId = socket.id;
    connectionStatus.querySelector(".dot").className = "dot connected";
    statusText.textContent = "Signaling Connected";
  });

  socket.on("disconnect", () => {
    connectionStatus.querySelector(".dot").className = "dot disconnected";
    statusText.textContent = "Disconnected";
  });

  socket.on("room-peers", ({ peers, yourId }) => {
    myPeerId = yourId;
    updatePeerList(peers);
    peers.forEach(peerId => createPeerConnection(peerId, true));
  });

  socket.on("peer-joined", ({ peerId }) => {
    updatePeerList([...getPeerIds(), peerId]);
  });

  socket.on("peer-left", ({ peerId }) => {
    if (peerConnections.has(peerId)) {
      peerConnections.get(peerId).close();
      peerConnections.delete(peerId);
      dataChannels.delete(peerId);
    }
    updatePeerList(getPeerIds().filter(id => id !== peerId));
  });

  socket.on("signal-offer", async ({ senderPeerId, offer }) => {
    const pc = createPeerConnection(senderPeerId, false);
    await pc.setRemoteDescription(new RTCSessionDescription(offer));
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    socket.emit("signal-answer", { targetPeerId: senderPeerId, answer });
  });

  socket.on("signal-answer", async ({ senderPeerId, answer }) => {
    if (peerConnections.has(senderPeerId)) {
      const pc = peerConnections.get(senderPeerId);
      await pc.setRemoteDescription(new RTCSessionDescription(answer));
    }
  });

  socket.on("ice-candidate", async ({ senderPeerId, candidate }) => {
    if (peerConnections.has(senderPeerId)) {
      const pc = peerConnections.get(senderPeerId);
      await pc.addIceCandidate(new RTCIceCandidate(candidate));
    }
  });
}

// -----------------------------------------------------------------------------
// 3. WebRTC Peer Connection Setup
// -----------------------------------------------------------------------------
function createPeerConnection(targetPeerId, isInitiator) {
  if (peerConnections.has(targetPeerId)) {
    return peerConnections.get(targetPeerId);
  }

  const pc = new RTCPeerConnection(rtcConfig);
  peerConnections.set(targetPeerId, pc);

  pc.onicecandidate = (event) => {
    if (event.candidate) {
      socket.emit("ice-candidate", {
        targetPeerId,
        candidate: event.candidate
      });
    }
  };

  pc.onconnectionstatechange = () => {
    if (pc.connectionState === "connected") {
      updateRTTMetric(pc);
      statusText.textContent = "Direct UDP Active";
    }
  };

  if (isInitiator) {
    const dc = pc.createDataChannel("file-transfer", { ordered: true });
    setupDataChannel(dc, targetPeerId);
    dataChannels.set(targetPeerId, dc);

    pc.createOffer().then(offer => {
      pc.setLocalDescription(offer);
      socket.emit("signal-offer", { targetPeerId, offer });
    });
  } else {
    pc.ondatachannel = (event) => {
      const dc = event.channel;
      setupDataChannel(dc, targetPeerId);
      dataChannels.set(targetPeerId, dc);
    };
  }

  return pc;
}

// -----------------------------------------------------------------------------
// 4. 100% Serverless Manual Copy-Paste Handshake (Mode 2)
// -----------------------------------------------------------------------------
btnCreateOffer.addEventListener("click", async () => {
  serverlessPeerConnection = new RTCPeerConnection(rtcConfig);
  const pc = serverlessPeerConnection;

  const dc = pc.createDataChannel("file-transfer", { ordered: true });
  setupDataChannel(dc, "ServerlessPeer");
  dataChannels.set("ServerlessPeer", dc);

  pc.onconnectionstatechange = () => {
    if (pc.connectionState === "connected") {
      updateRTTMetric(pc);
      connectionStatus.querySelector(".dot").className = "dot connected";
      statusText.textContent = "Serverless UDP Connected!";
    }
  };

  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);

  await new Promise((resolve) => {
    if (pc.iceGatheringState === "complete") resolve();
    else pc.onicecandidate = (e) => !e.candidate && resolve();
  });

  const offerPayload = btoa(JSON.stringify(pc.localDescription));
  localOfferText.value = offerPayload;
  btnCopyOffer.disabled = false;
});

btnCreateAnswer.addEventListener("click", async () => {
  const rawOffer = remoteOfferText.value.trim();
  if (!rawOffer) return alert("Please paste the Sender's Offer Code!");

  serverlessPeerConnection = new RTCPeerConnection(rtcConfig);
  const pc = serverlessPeerConnection;

  pc.ondatachannel = (event) => {
    const dc = event.channel;
    setupDataChannel(dc, "ServerlessPeer");
    dataChannels.set("ServerlessPeer", dc);
  };

  pc.onconnectionstatechange = () => {
    if (pc.connectionState === "connected") {
      updateRTTMetric(pc);
      connectionStatus.querySelector(".dot").className = "dot connected";
      statusText.textContent = "Serverless UDP Connected!";
    }
  };

  const offerDesc = JSON.parse(atob(rawOffer));
  await pc.setRemoteDescription(new RTCSessionDescription(offerDesc));

  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);

  await new Promise((resolve) => {
    if (pc.iceGatheringState === "complete") resolve();
    else pc.onicecandidate = (e) => !e.candidate && resolve();
  });

  const answerPayload = btoa(JSON.stringify(pc.localDescription));
  localAnswerText.value = answerPayload;
  btnCopyAnswer.disabled = false;
});

btnConnectAnswer.addEventListener("click", async () => {
  const rawAnswer = remoteAnswerText.value.trim();
  if (!rawAnswer) return alert("Please paste the Receiver's Answer Code!");
  if (!serverlessPeerConnection) return alert("Please generate an Offer first!");

  const answerDesc = JSON.parse(atob(rawAnswer));
  await serverlessPeerConnection.setRemoteDescription(new RTCSessionDescription(answerDesc));
});

btnCopyOffer.addEventListener("click", () => {
  navigator.clipboard.writeText(localOfferText.value);
  btnCopyOffer.textContent = "✅ Offer Copied!";
  setTimeout(() => btnCopyOffer.textContent = "📋 Copy Offer Code", 2000);
});

btnCopyAnswer.addEventListener("click", () => {
  navigator.clipboard.writeText(localAnswerText.value);
  btnCopyAnswer.textContent = "✅ Answer Copied!";
  setTimeout(() => btnCopyAnswer.textContent = "📋 Copy Answer Code", 2000);
});

// -----------------------------------------------------------------------------
// 5. DataChannel Events (Receive Engine & Cancel Signal Listener)
// -----------------------------------------------------------------------------
function setupDataChannel(dc, peerId) {
  dc.binaryType = "arraybuffer";

  dc.onopen = () => {
    console.log(`[DataChannel Open] Direct UDP Pipeline active with ${peerId}`);
    if (selectedFiles.length > 0) {
      btnSend.disabled = false;
    }
  };

  dc.onmessage = (event) => {
    const data = event.data;
    if (typeof data === "string") {
      const msg = JSON.parse(data);

      if (msg.type === "cancel-file") {
        // Selective cancel/skip of current file by sender
        receivedChunks = [];
        receivedBytes = 0;
        stopReceiveSpeedMonitor();

        receiveTitle.textContent = `⏩ Skipped "${msg.fileName}" by Sender`;
        receiveProgressBar.style.width = "0%";
        receiveStatus.textContent = "Skipped";
        return;
      }

      if (msg.type === "cancel-transfer") {
        // Global cancel batch by sender
        receivedChunks = [];
        receivedBytes = 0;
        stopReceiveSpeedMonitor();

        receiveTitle.textContent = "❌ Batch Cancelled by Sender";
        receiveProgressBar.style.width = "0%";
        receivePercent.textContent = "0%";
        receiveStatus.textContent = "Cancelled";
        return;
      }

      if (msg.type === "metadata") {
        incomingMetadata = msg;
        receivedChunks = [];
        receivedBytes = 0;
        receiveStartTime = Date.now();
        receiveBytesTransferred = 0;

        receiveDashboard.classList.remove("hidden");
        receiveTitle.textContent = `Downloading [${msg.fileIndex + 1}/${msg.totalFiles}] "${msg.name}"...`;
        receivePercent.textContent = `0%`;
        receiveProgressBar.style.width = `0%`;
        receiveStatus.textContent = "Receiving";

        startReceiveSpeedMonitor();
      }
    } else {
      receivedChunks.push(data);
      receivedBytes += data.byteLength;
      receiveBytesTransferred += data.byteLength;

      const progress = Math.min(100, Math.floor((receivedBytes / incomingMetadata.size) * 100));
      receiveProgressBar.style.width = `${progress}%`;
      receivePercent.textContent = `${progress}%`;
      receiveChunks.textContent = `File ${incomingMetadata.fileIndex + 1}/${incomingMetadata.totalFiles}`;

      if (receivedBytes >= incomingMetadata.size) {
        completeSingleFileDownload();
      }
    }
  };
}

// -----------------------------------------------------------------------------
// 6. Sender Engine: Multi-File Streaming with Selective Skip & Cancel Controls
// -----------------------------------------------------------------------------
async function queueAndSendFiles() {
  if (selectedFiles.length === 0 || dataChannels.size === 0) return;

  cancelSendRequested = false;
  skipFileRequested = false;

  outgoingQueue.push(...selectedFiles);
  selectedFiles = [];
  fileDetails.classList.add("hidden");

  if (isSending) {
    sendTitle.textContent = `Uploading... (${outgoingQueue.length} queued)`;
    return;
  }

  isSending = true;
  sendDashboard.classList.remove("hidden");

  let initialTotal = outgoingQueue.length;
  let processedCount = 0;

  while (outgoingQueue.length > 0 && !cancelSendRequested) {
    const currentFile = outgoingQueue.shift();
    processedCount++;
    skipFileRequested = false;

    const result = await sendSingleFile(currentFile, processedCount - 1, initialTotal);
    if (cancelSendRequested) break;
  }

  isSending = false;
  if (cancelSendRequested) {
    sendTitle.textContent = `❌ Upload Batch Cancelled`;
    sendProgressBar.style.width = `0%`;
    sendPercent.textContent = `0%`;
  } else {
    sendTitle.textContent = `✅ All ${processedCount} Files Processed!`;
  }
  stopSendSpeedMonitor();
  btnSend.disabled = false;
}

function sendSingleFile(file, fileIndex, totalFiles) {
  return new Promise((resolve) => {
    sendTitle.textContent = `Uploading [${fileIndex + 1}/${totalFiles}] "${file.name}"...`;
    sendProgressBar.style.width = `0%`;

    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    sendBytesTransferred = 0;
    sendStartTime = Date.now();
    startSendSpeedMonitor();

    const metadata = JSON.stringify({
      type: "metadata",
      name: file.name,
      size: file.size,
      mimeType: file.type || "application/octet-stream",
      totalChunks,
      fileIndex,
      totalFiles
    });

    dataChannels.forEach(dc => dc.readyState === "open" && dc.send(metadata));

    let currentChunk = 0;
    const fileReader = new FileReader();
    let offset = 0;

    fileReader.onload = async (e) => {
      if (cancelSendRequested || skipFileRequested) {
        return resolve(false);
      }

      const chunkBuffer = e.target.result;
      
      for (const [peerId, dc] of dataChannels.entries()) {
        if (dc.readyState === "open") {
          if (dc.bufferedAmount > dc.bufferedAmountLowThreshold) {
            await new Promise(res => dc.onbufferedamountlow = res);
          }
          dc.send(chunkBuffer);
        }
      }

      sendBytesTransferred += chunkBuffer.byteLength;
      currentChunk++;
      offset += chunkBuffer.byteLength;

      const progress = Math.min(100, Math.floor((offset / file.size) * 100));
      sendProgressBar.style.width = `${progress}%`;
      sendPercent.textContent = `${progress}%`;
      sendChunks.textContent = `File ${fileIndex + 1}/${totalFiles}`;

      if (offset < file.size && !cancelSendRequested && !skipFileRequested) {
        readNextChunk();
      } else {
        setTimeout(() => resolve(true), 100);
      }
    };

    function readNextChunk() {
      const slice = file.slice(offset, offset + CHUNK_SIZE);
      fileReader.readAsArrayBuffer(slice);
    }

    readNextChunk();
  });
}

// Selective Skip Action Handler (Skips active file, moves to next file in queue)
btnSkipFile.addEventListener("click", () => {
  if (!isSending) return;

  skipFileRequested = true;
  const skipPayload = JSON.stringify({ type: "cancel-file", fileName: sendTitle.textContent });
  dataChannels.forEach(dc => dc.readyState === "open" && dc.send(skipPayload));

  sendTitle.textContent = `⏩ Skipped file! Moving to next...`;
});

// Global Cancel Batch Action Handler
btnCancelSend.addEventListener("click", () => {
  cancelSendRequested = true;
  outgoingQueue = [];
  
  const cancelPayload = JSON.stringify({ type: "cancel-transfer" });
  dataChannels.forEach(dc => dc.readyState === "open" && dc.send(cancelPayload));

  isSending = false;
  stopSendSpeedMonitor();
  sendTitle.textContent = "❌ Upload Batch Cancelled";
  sendProgressBar.style.width = "0%";
  sendPercent.textContent = "0%";
  btnSend.disabled = false;
});

btnCancelReceive.addEventListener("click", () => {
  receivedChunks = [];
  receivedBytes = 0;
  stopReceiveSpeedMonitor();

  receiveTitle.textContent = "❌ Download Cancelled by Receiver";
  receiveProgressBar.style.width = "0%";
  receivePercent.textContent = "0%";
  receiveStatus.textContent = "Cancelled";
});

// -----------------------------------------------------------------------------
// 7. File Assembly & Multi-Download Link Generator
// -----------------------------------------------------------------------------
function completeSingleFileDownload() {
  stopReceiveSpeedMonitor();

  const blob = new Blob(receivedChunks, { type: incomingMetadata.mimeType });
  const downloadUrl = URL.createObjectURL(blob);

  downloadArea.classList.remove("hidden");

  const downloadBtn = document.createElement("a");
  downloadBtn.className = "btn btn-download";
  downloadBtn.href = downloadUrl;
  downloadBtn.download = incomingMetadata.name;
  downloadBtn.textContent = `📥 Download "${incomingMetadata.name}" (${(incomingMetadata.size / (1024 * 1024)).toFixed(2)} MB)`;

  downloadsList.appendChild(downloadBtn);

  if (incomingMetadata.fileIndex + 1 === incomingMetadata.totalFiles) {
    receiveTitle.textContent = `✅ Downloaded All ${incomingMetadata.totalFiles} Files!`;
    receiveProgressBar.style.width = `100%`;
    receivePercent.textContent = `100%`;
    receiveStatus.textContent = "Complete";
  }
}

// -----------------------------------------------------------------------------
// 8. Independent Speed Monitors & RTT Helpers
// -----------------------------------------------------------------------------
function startSendSpeedMonitor() {
  stopSendSpeedMonitor();
  sendSpeedInterval = setInterval(() => {
    const elapsedSeconds = (Date.now() - sendStartTime) / 1000;
    if (elapsedSeconds > 0) {
      const speedMBps = (sendBytesTransferred / (1024 * 1024)) / elapsedSeconds;
      sendSpeed.textContent = `${speedMBps.toFixed(2)} MB/s`;
    }
  }, 300);
}

function stopSendSpeedMonitor() {
  if (sendSpeedInterval) {
    clearInterval(sendSpeedInterval);
    sendSpeedInterval = null;
  }
}

function startReceiveSpeedMonitor() {
  stopReceiveSpeedMonitor();
  receiveSpeedInterval = setInterval(() => {
    const elapsedSeconds = (Date.now() - receiveStartTime) / 1000;
    if (elapsedSeconds > 0) {
      const speedMBps = (receiveBytesTransferred / (1024 * 1024)) / elapsedSeconds;
      receiveSpeed.textContent = `${speedMBps.toFixed(2)} MB/s`;
    }
  }, 300);
}

function stopReceiveSpeedMonitor() {
  if (receiveSpeedInterval) {
    clearInterval(receiveSpeedInterval);
    receiveSpeedInterval = null;
  }
}

async function updateRTTMetric(pc) {
  try {
    const stats = await pc.getStats();
    stats.forEach(report => {
      if (report.type === "candidate-pair" && report.state === "succeeded" && report.currentRoundTripTime) {
        const rttMs = Math.round(report.currentRoundTripTime * 1000);
        metricRtt.textContent = `${rttMs} ms`;
      }
    });
  } catch (err) {
    console.error(err);
  }
}

function updatePeerList(peers) {
  peerCount.textContent = `${peers.length} Peers`;
  if (peers.length === 0) {
    peerList.innerHTML = `<li class="empty-peer">No peers connected. Share your Channel ID!</li>`;
  } else {
    peerList.innerHTML = peers.map(id => `
      <li class="peer-item">
        <span>Peer ${id.substring(0, 8)}...</span>
        <span class="badge-accent">Direct UDP Active</span>
      </li>
    `).join("");
  }
}

function getPeerIds() {
  return Array.from(peerConnections.keys());
}

// Event Listeners
btnJoin.addEventListener("click", () => {
  const room = roomIdInput.value.trim();
  if (room) {
    currentRoom = room;
    socket.emit("join-room", room);
  }
});

dropZone.addEventListener("click", () => fileInput.click());
dropZone.addEventListener("dragover", (e) => {
  e.preventDefault();
  dropZone.classList.add("dragover");
});
dropZone.addEventListener("dragleave", () => dropZone.classList.remove("dragover"));
dropZone.addEventListener("drop", (e) => {
  e.preventDefault();
  dropZone.classList.remove("dragover");
  if (e.dataTransfer.files.length > 0) {
    handleFileSelect(Array.from(e.dataTransfer.files));
  }
});

fileInput.addEventListener("change", (e) => {
  if (e.target.files.length > 0) {
    handleFileSelect(Array.from(e.target.files));
  }
});

function handleFileSelect(files) {
  if (isSending) {
    outgoingQueue.push(...files);
    sendTitle.textContent = `Uploading... (${outgoingQueue.length} queued)`;
    return;
  }

  selectedFiles = files;
  renderSelectedFilesList();
}

function renderSelectedFilesList() {
  let totalBytes = 0;
  selectedFileList.innerHTML = "";

  selectedFiles.forEach((file, index) => {
    totalBytes += file.size;
    const li = document.createElement("li");
    li.className = "file-item";
    li.innerHTML = `
      <div class="file-item-left">
        <span>📄</span>
        <span class="file-item-name">${file.name}</span>
      </div>
      <div class="file-item-right">
        <span class="file-item-size">${(file.size / (1024 * 1024)).toFixed(2)} MB</span>
        <button class="btn-remove-file" data-index="${index}" title="Remove file from list">✖</button>
      </div>
    `;
    selectedFileList.appendChild(li);
  });

  // Attach individual file remove click handlers
  document.querySelectorAll(".btn-remove-file").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const removeIndex = parseInt(btn.getAttribute("data-index"), 10);
      selectedFiles.splice(removeIndex, 1);
      if (selectedFiles.length === 0) {
        fileDetails.classList.add("hidden");
      } else {
        renderSelectedFilesList();
      }
    });
  });

  batchCount.textContent = `${selectedFiles.length} File${selectedFiles.length > 1 ? "s" : ""} Selected`;
  fileSizeEl.textContent = `Total: ${(totalBytes / (1024 * 1024)).toFixed(2)} MB`;
  fileDetails.classList.remove("hidden");
  btnSend.disabled = dataChannels.size === 0 || selectedFiles.length === 0;
}

btnSend.addEventListener("click", queueAndSendFiles);

// Initialize on Load
initSignaling();
