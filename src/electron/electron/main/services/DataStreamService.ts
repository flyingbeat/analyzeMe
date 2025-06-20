import * as http from 'http';
import * as os from 'os';
import { WebSocket, WebSocketServer } from 'ws';
import getMainLogger from '../../config/Logger';

const LOG = getMainLogger('DataStreamService');

interface WebSocketMessage {
  type: string;
  content: any;
  timestamp: string;
  clientId?: string;
}

interface ClientData {
  id: string;
  ws: WebSocket;
  intervals: Set<NodeJS.Timeout>;
  metadata?: any;
}

export class DataStreamService {
  private server: http.Server;
  private wss: WebSocketServer;
  private clients: Map<WebSocket, ClientData> = new Map();
  private isRunning: boolean = false;
  private port: number;
  private host: string;

  constructor(port: number = 3000, host: string = '0.0.0.0') {
    this.port = port;
    this.host = host;
  }

  private setupWebSocketHandlers(): void {
    this.wss.on('connection', (ws: WebSocket, req) => {
      const clientId = this.generateClientId();
      const clientData: ClientData = {
        id: clientId,
        ws,
        intervals: new Set(),
        metadata: {
          connectedAt: new Date().toISOString(),
          userAgent: req.headers['user-agent'],
          ip: req.socket.remoteAddress
        }
      };

      this.clients.set(ws, clientData);
      LOG.info(`New client connected: ${clientId} (Total: ${this.clients.size})`);

      // Send welcome message
      this.sendToClient(ws, {
        type: 'welcome',
        content: 'Connected to DataStream Service',
        timestamp: new Date().toISOString(),
        clientId
      });

      // Set up message handler
      ws.on('message', (data: Buffer) => {
        this.handleClientMessage(ws, data);
      });

      // Set up close handler
      ws.on('close', () => {
        this.handleClientDisconnect(ws);
      });

      // Set up error handler
      ws.on('error', (error: Error) => {
        LOG.error(`WebSocket error for client ${clientId}:`, error);
        this.handleClientDisconnect(ws);
      });

      // Set up ping/pong for connection health
      this.setupHeartbeat(ws);
    });
  }

  private generateClientId(): string {
    return `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private handleClientMessage(ws: WebSocket, data: Buffer): void {
    try {
      const message: WebSocketMessage = JSON.parse(data.toString());
      const clientData = this.clients.get(ws);

      if (!clientData) return;

      LOG.debug(`Received from ${clientData.id}:`, message);

      // Handle different message types
      switch (message.type) {
        case 'ping':
          this.sendToClient(ws, {
            type: 'pong',
            content: 'pong',
            timestamp: new Date().toISOString()
          });
          break;

        case 'subscribe':
          this.handleSubscription(ws, message);
          break;

        case 'unsubscribe':
          this.handleUnsubscription(ws, message);
          break;

        default:
          // Echo message back with server response
          this.sendToClient(ws, {
            type: 'echo',
            content: `Server received: "${message.content}"`,
            timestamp: new Date().toISOString()
          });

          // Handle specific content
          if (message.content?.toLowerCase().includes('hello')) {
            setTimeout(() => {
              this.sendToClient(ws, {
                type: 'message',
                content: 'Hello there! How can I help you?',
                timestamp: new Date().toISOString()
              });
            }, 1000);
          }
          break;
      }
    } catch (error) {
      LOG.error('Error parsing message:', error);
      this.sendToClient(ws, {
        type: 'error',
        content: 'Invalid message format',
        timestamp: new Date().toISOString()
      });
    }
  }

  private handleSubscription(ws: WebSocket, message: WebSocketMessage): void {
    const clientData = this.clients.get(ws);
    if (!clientData) return;

    const topic = message.content;

    // Example: Set up periodic data streaming for different topics
    if (topic === 'system_stats') {
      const interval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          this.sendToClient(ws, {
            type: 'system_stats',
            content: {
              memory: process.memoryUsage(),
              uptime: process.uptime(),
              clients: this.clients.size
            },
            timestamp: new Date().toISOString()
          });
        }
      }, 5000);
      clientData.intervals.add(interval);
    }

    this.sendToClient(ws, {
      type: 'subscription_ack',
      content: `Subscribed to ${topic}`,
      timestamp: new Date().toISOString()
    });
  }

  private handleUnsubscription(ws: WebSocket, message: WebSocketMessage): void {
    const clientData = this.clients.get(ws);
    if (!clientData) return;

    // Clear all intervals for this client (simplified approach)
    clientData.intervals.forEach((interval) => clearInterval(interval));
    clientData.intervals.clear();

    this.sendToClient(ws, {
      type: 'unsubscription_ack',
      content: `Unsubscribed from ${message.content}`,
      timestamp: new Date().toISOString()
    });
  }

  private setupHeartbeat(ws: WebSocket): void {
    const clientData = this.clients.get(ws);
    if (!clientData) return;

    const heartbeat = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.ping();
      }
    }, 30000);

    clientData.intervals.add(heartbeat);
  }

  private handleClientDisconnect(ws: WebSocket): void {
    const clientData = this.clients.get(ws);
    if (clientData) {
      LOG.info(`Client disconnected: ${clientData.id}`);

      // Clear all intervals
      clientData.intervals.forEach((interval) => clearInterval(interval));
      this.clients.delete(ws);
    }
  }

  // Public Methods

  public async init() {
    this.wss = new WebSocketServer({ port: this.port, host: this.host });
    this.setupWebSocketHandlers();
  }

  public async stop(): Promise<void> {
    return new Promise((resolve) => {
      // Close all client connections
      this.clients.forEach((clientData) => {
        clientData.intervals.forEach((interval) => clearInterval(interval));
        clientData.ws.close();
      });
      this.clients.clear();

      // Close WebSocket server
      this.wss.close(() => {
        // Close HTTP server
        this.server.close(() => {
          this.isRunning = false;
          LOG.info('DataStream Server stopped');
          resolve();
        });
      });
    });
  }

  public broadcast(message: WebSocketMessage): void {
    const messageString = JSON.stringify({
      ...message,
      timestamp: message.timestamp || new Date().toISOString()
    });

    this.clients.forEach((clientData) => {
      if (clientData.ws.readyState === WebSocket.OPEN) {
        clientData.ws.send(messageString);
      }
    });
  }

  public sendToClient(ws: WebSocket, message: WebSocketMessage): void {
    if (ws.readyState === WebSocket.OPEN) {
      const messageString = JSON.stringify({
        ...message,
        timestamp: message.timestamp || new Date().toISOString()
      });
      ws.send(messageString);
    }
  }

  public sendToClientById(clientId: string, message: WebSocketMessage): boolean {
    const clientEntries = Array.from(this.clients.values());
    for (const clientData of clientEntries) {
      if (clientData.id === clientId) {
        this.sendToClient(clientData.ws, message);
        return true;
      }
    }
    return false;
  }

  public getConnectedClients(): Array<{ id: string; metadata: any }> {
    return Array.from(this.clients.values()).map((client) => ({
      id: client.id,
      metadata: client.metadata
    }));
  }

  public getClientCount(): number {
    return this.clients.size;
  }

  public isServerRunning(): boolean {
    return this.isRunning;
  }

  public getServerInfo(): {
    port: number;
    host: string;
    localIP: string;
    isRunning: boolean;
    clientCount: number;
  } {
    return {
      port: this.port,
      host: this.host,
      localIP: this.getLocalIP(),
      isRunning: this.isRunning,
      clientCount: this.clients.size
    };
  }

  private getLocalIP(): string {
    const interfaces = os.networkInterfaces();

    for (const name of Object.keys(interfaces)) {
      const networkInterface = interfaces[name];
      if (networkInterface) {
        for (const iface of networkInterface) {
          // Skip over non-IPv4 and internal (i.e. 127.0.0.1) addresses
          if (iface.family === 'IPv4' && !iface.internal) {
            return iface.address;
          }
        }
      }
    }
    return 'localhost';
  }
}
