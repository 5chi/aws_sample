package sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;


//S3のputをトリガーにdynamoDBにファイルの内容を書き出す。
//S3にupするファイルはJSONデータ。(他アプリで作成されたJSONデータを流すイメージ)

public class S3PutDynamoWriteDemo implements RequestHandler<S3Event, Object> {

    @Override
    public Object handleRequest(S3Event input, Context context) {

    	AmazonS3Client client = new AmazonS3Client();
    	client.setRegion(Region.getRegion(Regions.AP_NORTHEAST_1));
    	String bucketName = input.getRecords().get(0).getS3().getBucket().getName();
    	String keyName = input.getRecords().get(0).getS3().getObject().getKey();

    	S3Object s3object = client.getObject(new GetObjectRequest(bucketName, keyName));
    	String data = "";
		data = readData(s3object);
    	context.getLogger().log(data);

    	//testテーブル、primaryは"data_id"として作成済み。
        DynamoDB db = new DynamoDB(new AmazonDynamoDBClient().withRegion(Regions.AP_NORTHEAST_1));
        Table table = db.getTable("test");

        Item item = new Item().withPrimaryKey("data_id", new Date().getTime()).withString("document", data);
        table.putItem(item);

        return null;

    }

    private String readData(S3Object object) {
    	StringBuilder readData = new StringBuilder();
    	try (BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()))) {
    		br.lines().forEach(line -> {readData.append(line);});
    	}catch(IOException e) {
    		System.err.println(e.getMessage());
    	}
    	return readData.toString();
    }
}
