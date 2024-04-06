package ds.tutorial.communication.client;

import java.io.IOException;
import java.util.Scanner;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import ds.tutorial.communication.grpc.generated.*;
import ds.tutorial.naming.client.NameServiceClient;

public class CheckBalanceServiceClient {
    public static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";
    private ManagedChannel channel = null;
    BalanceServiceGrpc.BalanceServiceBlockingStub clientStub = null;
    String host = null;
    int port = -1;

    public static void main(String[] args) throws InterruptedException, IOException {
        
        CheckBalanceServiceClient client = new CheckBalanceServiceClient();
        client.initializeConnection();
        client.processUserRequests();
        client.closeConnection();

    }

    public CheckBalanceServiceClient () throws IOException, InterruptedException {
        fetchServerDetails();

    }
    
    private void fetchServerDetails() throws IOException, InterruptedException {
       NameServiceClient client = new NameServiceClient(NAME_SERVICE_ADDRESS);
       NameServiceClient.ServiceDetails serviceDetails = client.findService("CheckBalanceService");
       host = serviceDetails.getIPAddress();
       port = serviceDetails.getPort();
   }


    private void initializeConnection () {
        System.out.println("Initializing Connecting to server at " + host + ":" + port);
        channel = ManagedChannelBuilder.forAddress("localhost", 11436)
                .usePlaintext()
                .build();
        clientStub = BalanceServiceGrpc.newBlockingStub(channel);
        channel.getState(true);
    }

    private void closeConnection() {
        channel.shutdown();
    }

    private void processUserRequests() throws InterruptedException, IOException {

        while (true) {
            Scanner userInput = new Scanner(System.in);
            System.out.println("\nEnter Account ID to check the balance :");

            String accountId = userInput.nextLine().trim();
            System.out.println("Requesting server to check the account balance for " + accountId.toString());
            CheckBalanceRequest request = CheckBalanceRequest
                    .newBuilder()
                    .setAccountId(accountId)
                    .build();
            ConnectivityState state = channel.getState(true);
            while (state != ConnectivityState.READY) {
               System.out.println("Service unavailable, looking for a service provider..");
               fetchServerDetails();
               initializeConnection();
               Thread.sleep(5000);
               state = channel.getState(true);
           }

            CheckBalanceResponse response = clientStub.checkBalance(request);
            System.out.printf("My balance is " + response.getBalance() + " LKR");

            Thread.sleep(1000);
        }
    }
}

