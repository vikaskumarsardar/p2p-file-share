const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const path = require("path");

app.use(cors());
app.use(express.static(__dirname));
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// Store room connections: roomName -> Set of socket IDs
const rooms = new Map();

io.on("connection", (socket) => {
  console.log(`[P2P Signaling] Peer Connected: ${socket.id}`);

  // 1. Peer Joins Room
  socket.on("join-room", (roomName) => {
    socket.join(roomName);
    socket.currentRoom = roomName;

    if (!rooms.has(roomName)) {
      rooms.set(roomName, new Set());
    }
    rooms.get(roomName).add(socket.id);

    const otherPeers = Array.from(rooms.get(roomName)).filter(id => id !== socket.id);
    
    // Notify joining peer of existing room members
    socket.emit("room-peers", { peers: otherPeers, yourId: socket.id });

    // Notify existing members of new joining peer
    socket.to(roomName).emit("peer-joined", { peerId: socket.id });

    console.log(`[Room ${roomName}] Peer ${socket.id} joined. Total peers: ${rooms.get(roomName).size}`);
  });

  // 2. Relay WebRTC SDP Offer
  socket.on("signal-offer", ({ targetPeerId, offer }) => {
    console.log(`[Signal] Offer from ${socket.id} -> ${targetPeerId}`);
    io.to(targetPeerId).emit("signal-offer", {
      senderPeerId: socket.id,
      offer
    });
  });

  // 3. Relay WebRTC SDP Answer
  socket.on("signal-answer", ({ targetPeerId, answer }) => {
    console.log(`[Signal] Answer from ${socket.id} -> ${targetPeerId}`);
    io.to(targetPeerId).emit("signal-answer", {
      senderPeerId: socket.id,
      answer
    });
  });

  // 4. Relay STUN/ICE Candidate
  socket.on("ice-candidate", ({ targetPeerId, candidate }) => {
    io.to(targetPeerId).emit("ice-candidate", {
      senderPeerId: socket.id,
      candidate
    });
  });

  // 5. Handle Peer Disconnection
  socket.on("disconnect", () => {
    console.log(`[P2P Signaling] Peer Disconnected: ${socket.id}`);
    if (socket.currentRoom && rooms.has(socket.currentRoom)) {
      const roomSet = rooms.get(socket.currentRoom);
      roomSet.delete(socket.id);
      if (roomSet.size === 0) {
        rooms.delete(socket.currentRoom);
      } else {
        socket.to(socket.currentRoom).emit("peer-left", { peerId: socket.id });
      }
    }
  });
});

const PORT = process.env.PORT || 7000;
server.listen(PORT, () => {
  console.log(`=======================================================`);
  console.log(` 🚀 P2P FILE SHARE SIGNALING SERVER RUNNING`);
  console.log(` 🌐 URL: http://localhost:${PORT}`);
  console.log(`=======================================================`);
});
