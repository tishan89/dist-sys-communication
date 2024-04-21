package ds.tutorial.communication.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.KeeperException;

import ds.tutorial.synchronization.client.DistributedLock;
import ds.tutorial.synchronization.client.DistributedTx;
import ds.tutorial.synchronization.client.DistributedTxCoordinator;
import ds.tutorial.synchronization.client.DistributedTxParticipant;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class BankServer {
    private DistributedLock leaderLock;
    private int serverPort;
    private AtomicBoolean isLeader = new AtomicBoolean(false);
    private byte[] leaderData;
    private Map<String, Double> accounts = new HashMap();
    DistributedTx transaction;
   SetBalanceServiceImpl setBalanceService;
   BalanceServiceImpl checkBalanceService;
    public static void main (String[] args) throws Exception{
        DistributedLock.setZooKeeperURL("localhost:2181");
        DistributedTx.setZooKeeperURL("localhost:2181");
        int serverPort;
        if (args.length != 1) {
            System.out.println("Usage BankServer <port>");
            System.exit(1);
        }
        serverPort = Integer.parseInt(args[0].trim());
        BankServer server = new BankServer("localhost", serverPort);
        server.startServer();
    }
    
    public BankServer(String host, int port) throws InterruptedException, IOException, KeeperException {
        this.serverPort = port;
        leaderLock = new DistributedLock("BankServerTestCluster", buildServerData(host, port));
        setBalanceService = new SetBalanceServiceImpl(this);
         checkBalanceService = new BalanceServiceImpl(this);
         transaction = new DistributedTxParticipant(setBalanceService);
     }
     
     public void startServer() throws IOException, InterruptedException, KeeperException {
        Server server = ServerBuilder
                .forPort(serverPort)
                .addService(setBalanceService)
                .addService(checkBalanceService)
                .build();
        server.start();
        System.out.println("BankServer Started and ready to accept requests on port " + serverPort);
     
        tryToBeLeader();
        server.awaitTermination();
     }
     
     public static String buildServerData(String IP, int port) {
        StringBuilder builder = new StringBuilder();
        builder.append(IP).append(":").append(port);
        return builder.toString();
     }
     
     public boolean isLeader() {
        return isLeader.get();
     }

     public void setAccountBalance(String accountId, double value) {
        accounts.put(accountId, value);
     }
     
     public double getAccountBalance(String accountId) {
        Double value = accounts.get(accountId);
        return (value != null) ? value : 0.0;
     }
     
     public synchronized String[] getCurrentLeaderData() {
        return new String(leaderData).split(":");
     }
     
     public List<String[]> getOthersData() throws KeeperException, InterruptedException {
        List<String[]> result = new ArrayList<>();
        List<byte[]> othersData = leaderLock.getOthersData();
     
        for (byte[] data : othersData) {
            String[] dataStrings = new String(data).split(":");
            result.add(dataStrings);
        }
        return result;
     }
     
     public DistributedTx getTransaction() {
      return transaction;
      }
   
     
     private synchronized void setCurrentLeaderData(byte[] leaderData) {
        this.leaderData = leaderData;
     }

     private void tryToBeLeader() throws KeeperException, InterruptedException {
        Thread leaderCampaignThread = new Thread(new LeaderCampaignThread());
        leaderCampaignThread.start();
     }


     class LeaderCampaignThread implements Runnable {
        private byte[] currentLeaderData = null;
        @Override
        public void run() {
            System.out.println("Starting the leader Campaign");
     
            try {
                boolean leader = leaderLock.tryAcquireLock();
     
                while (!leader) {
                    byte[] leaderData = leaderLock.getLockHolderData();
                    if (currentLeaderData != leaderData) {
                        currentLeaderData = leaderData;
                        setCurrentLeaderData(currentLeaderData);
                    }
                    Thread.sleep(10000);
                    leader = leaderLock.tryAcquireLock();
                }
               //  System.out.println("I got the leader lock. Now acting as primary");
               //  isLeader.set(true);
                currentLeaderData = null;
                beTheLeader();
            } catch (Exception e){
            }
        }
     }

     private void beTheLeader() {
      System.out.println("I got the leader lock. Now acting as primary");
      isLeader.set(true);
      transaction = new DistributedTxCoordinator(setBalanceService);
   }
   
     
     
     
     
}

