package ds.tutorial.communication.server;


import ds.tutorial.communication.grpc.generated.CheckBalanceServiceGrpc;
import ds.tutorial.communication.grpc.generated.CheckBalanceResponse;

public class BalanceServiceImpl extends CheckBalanceServiceGrpc.CheckBalanceServiceImplBase {

    private BankServer bankServer;

    public BalanceServiceImpl(BankServer bankServer) {
        this.bankServer = bankServer;
    }

    @Override
    public void checkBalance(ds.tutorial.communication.grpc.generated.CheckBalanceRequest request,
                             io.grpc.stub.StreamObserver<ds.tutorial.communication.grpc.generated.CheckBalanceResponse> responseObserver) {
        String accountId = request.getAccountId();

        System.out.println("Request received..");

        double balance = getAccountBalance(accountId);
        CheckBalanceResponse response = CheckBalanceResponse.newBuilder()
                .setBalance(balance)
                .build();

        System.out.println("Responding, balance for account " + accountId + " is " + balance);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private double getAccountBalance(String accountId) {
        return bankServer.getAccountBalance(accountId);

    }

}
