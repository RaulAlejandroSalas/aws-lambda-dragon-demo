package de.rsasoft.app;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class App implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final AWSSimpleSystemsManagement ssm = AWSSimpleSystemsManagementClientBuilder.defaultClient();
    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private static final Gson gsonInstance = new GsonBuilder().create();



    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent apiGatewayProxyRequestEvent, Context context) {
        String dragonData = readDragonData(apiGatewayProxyRequestEvent);
        return generateResponse(dragonData);
    }

    private APIGatewayProxyResponseEvent generateResponse(String dragonData) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(200);
        response.setBody(gsonInstance.toJson(dragonData));
        return response;
    }


    protected static String readDragonData(APIGatewayProxyRequestEvent requestEvent){
        Map<String,String> queryParams = requestEvent.getQueryStringParameters();
        String bucketName = getBucketName();
        String key = getKey();
        String query = getQuery(queryParams);
        SelectObjectContentRequest request = getJSONRequest(bucketName, key, query);
        return queryS3(request);
    }

    private static String queryS3(SelectObjectContentRequest request){
        final AtomicBoolean isResultComplete = new AtomicBoolean(false);
        //Calling S3
        SelectObjectContentResult result = s3Client.selectObjectContent(request);

        InputStream resultInputStream = result.getPayload().getRecordsInputStream(
                new SelectObjectContentEventVisitor() {
                    @Override
                    public void visit(SelectObjectContentEvent.StatsEvent event) {
                        System.out.println("Receiving Stats, Bytes Scanned: " + event.getDetails().getBytesScanned()
                        + " Bytes Processed: " + event.getDetails().getBytesProcessed());
                    }
                    //Informs that the request has finished successfully

                    @Override
                    public void visit(SelectObjectContentEvent.EndEvent event) {
                        isResultComplete.set(true);
                        System.out.println("Received End Event. Result is complete...");
                    }
                }
        );
        String text = null;
        try {
            text = IOUtils.toString(resultInputStream, StandardCharsets.UTF_8.name());
        }catch (IOException exception){
            System.out.println(exception.getMessage());
        }
        return text;
    }


    private static SelectObjectContentRequest getJSONRequest(String bucketName, String key, String query)
    {
        //Object te send a request into S3
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(bucketName);
        request.setKey(key);
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);

        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setJson(new JSONInput().withType("Document"));
        inputSerialization.setCompressionType(CompressionType.NONE);

        request.setInputSerialization(inputSerialization);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setJson(new JSONOutput());
        request.setOutputSerialization(outputSerialization);
        return request;
    }

    private static String getKey() {
        GetParameterRequest bucketParameterRequest = new GetParameterRequest().withName("dragon_data_file_name").withName("");
        GetParameterResult bucketResult = ssm.getParameter(bucketParameterRequest);
        return bucketResult.getParameter().getValue();

    }

    private static String getBucketName() {
        GetParameterRequest bucketParameterRequest = new GetParameterRequest().withName("dragon_data_bucket_name").withName("");
        GetParameterResult bucketResult = ssm.getParameter(bucketParameterRequest);
        return bucketResult.getParameter().getValue();
    }

    public static String getQuery(Map<String,String> queryParams)
    {
        if(queryParams !=null){
            if(queryParams.containsKey("family")){
                return "SELECT * FROM S3Object[*][*] s WHERE s.family_str = ' "+ queryParams.get("family") + "'";
            }else if(queryParams.containsKey("dragonName")){
                return "SELECT * FROM S3Object[*][*] s WHERE s.family_str = ' "+ queryParams.get("dragonName") + "'";
            }
        }
        return "SELECT * FROM s3object s";
    }


}
