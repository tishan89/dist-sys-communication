package ds.tutorial.communication.client;

import java.util.Scanner;
import java.util.Set;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import ds.tutorial.communication.grpc.generated.*;

public class CheckBalanceServiceClient {
    private ManagedChannel channel = null;
    CheckBalanceServiceGrpc.CheckBalanceServiceBlockingStub clientStub = null;
    SetBalanceServiceGrpc.SetBalanceServiceBlockingStub setBalanceClient = null;
    String host = null;
    int port = -1;
    String mode;

    public static void main(String[] args) throws InterruptedException {
        String host = null;
        int port = -1;
        String mode;
        if (args.length != 3) {
            System.out.println("Usage CheckBalanceServiceClient <host> <port>");
            System.exit(1);
        }
        host = args[0];
        port = Integer.parseInt(args[1].trim());
        mode = args[2].trim();


        CheckBalanceServiceClient client = new CheckBalanceServiceClient(host, port, mode);
        client.initializeConnection();
        client.processUserRequests();
        client.closeConnection();

    }

    public CheckBalanceServiceClient (String host, int port, String mode) {
        this.host = host;
        this.port = port;
        this.mode = mode;
    }

    private void initializeConnection () {
        System.out.println("Initializing Connecting to server at " + host + ":" + port);
        channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();
        clientStub = CheckBalanceServiceGrpc.newBlockingStub(channel);
        setBalanceClient = SetBalanceServiceGrpc.newBlockingStub(channel);
    }

    private void closeConnection() {
        channel.shutdown();
    }

    private void processUserRequests() throws InterruptedException {

        while (true) {
            if (mode.equals("c")) {

                Scanner userInput = new Scanner(System.in);
                System.out.println("\nEnter Account ID to check the balance :");

                String accountId = userInput.nextLine().trim();
                System.out.println("Requesting server to check the account balance for " + accountId.toString());
                CheckBalanceRequest request = CheckBalanceRequest
                        .newBuilder()
                        .setAccountId(accountId)
                        .build();

                CheckBalanceResponse response = clientStub.checkBalance(request);
                System.out.printf("My balance is " + response.getBalance() + " LKR");

                Thread.sleep(1000);
            } else {
                Scanner userInput = new Scanner(System.in);
                System.out.println("\nEnter Account ID,amount to set the balance :");

                String setBalanceInput = userInput.nextLine().trim();
                String accountId = setBalanceInput.split(",")[0];
                double amount = Double.parseDouble(setBalanceInput.split(",")[1]);
                System.out.println("Requesting server to set the account balance for " + accountId.toString() + " as " + amount + " LKR");
                SetBalanceRequest request = SetBalanceRequest
                        .newBuilder()
                        .setAccountId(accountId)
                        .setValue(amount)
                        .build();

                SetBalanceResponse response = setBalanceClient.setBalance(request);
                System.out.printf("Set balance request status is " + response.getStatus());

                Thread.sleep(1000);
            }
        }
    }
}

