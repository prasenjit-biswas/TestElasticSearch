package com.test;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

public class TestElasticSearch {
	public static void main(String[] args) {
		Node node  = null;
		try{
			
			/*Settings indexSettings = ImmutableSettings.settingsBuilder().put("number_of_shards", 1).put("number_of_replicas", 1).build();
			node  = nodeBuilder().settings(indexSettings).node();*/
			node  = nodeBuilder().node();
			Client client = node.client();

			/*List<Map<String, Object>> xmlMapList = TestDownloadXml.getQuestionMapForTestId("12446742152221500");
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			int count = 0;
			for(int i =0; i < xmlMapList.size(); i++){
				Map<String, Object> questionMap = xmlMapList.get(i);
				count = count+1;
				System.out.println(" index : "+count+" , questionMap : "+questionMap);
				bulkRequest.add(client.prepareIndex("eztest", "sourceinfo", Integer.toString(count)).setSource(questionMap));
			}

			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				System.out.println(" Getting failure for Bulk Response");
			}*/
			//client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
			/*for(int i = 0; i < xmlMapList.size(); i++){
				Map<String, String> questionMap = xmlMapList.get(i);
				System.out.println(" index : "+i+" , questionMap : "+questionMap);
				client.prepareIndex("eztest", "sourceinfo", String.valueOf(i+1))
				.setSource(questionMap).execute().actionGet();
		    }*/
		    
			/*client.prepareIndex("bprasen", "testTable", "1")
		.setSource(putJsonDocument("ElasticSearch: Java",
				"ElasticSeach provides Java API, thus it executes all operations " +
						"asynchronously by using client object..",
						new Date(),
						new String[]{"elasticsearch"},
				"Prasenjit Biswas")).execute().actionGet();//--working

		client.prepareIndex("bprasen", "testTable", "2")
		.setSource(putJsonDocument("Java Web Application and ElasticSearch (Video)",
				"Today, here I am for exemplifying the usage of ElasticSearch which is an open source, distributed " +
						"and scalable full text search engine and a data analysis tool in a Java web application.",
						new Date(),
						new String[]{"elasticsearch"},
				"bPrasen")).execute().actionGet();*/ //--working

			getDocument(client, "eztest", "sourceinfo", "1"); //--working
			//getDocument(client, "bprasen", "testTable", "2"); //--working
			//updateDocument(client, "bprasen", "testTable", "2", "author", "Epsita Biswas"); //-- working
			//updateDocument(client, "bprasen", "testTable", "1", "tags", new String[]{"bigdata"}); //--not working
			//searchDocument(client, "eztest", "sourceinfo", "displayType", "Check All That Apply"); //--working
			//deleteDocument(client, "kodcucom", "article", "1");

			/*client.prepareIndex("mcgrawhill", "eztest", "1")
		.setSource(putJsonDocument("International Business",
				"International Business",
						new Date(),
						new String[]{"elasticsearch"},
				"Suman Karmakar")).execute().actionGet();

		client.prepareIndex("mcgrawhill", "eztest", "2")
		.setSource(putJsonDocument("Core Concepts in Health",
				"Core Concepts in Health",
						new Date(),
						new String[]{"elasticsearch"},
				"Prasenjit Biswas")).execute().actionGet();

		client.prepareIndex("mcgrawhill", "eztest", "3")
		.setSource(putJsonDocument("Chemistry: The Molecular Nature of Matter and Change",
				"Chemistry: The Molecular Nature of Matter and Change",
						new Date(),
						new String[]{"elasticsearch"},
				"Biplab Mondal")).execute().actionGet();*/

		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			node.close();	
		}
	}

	public static Map<String, Object> putJsonDocument(String title, String content, Date postDate, 
			String[] tags, String author){

		Map<String, Object> jsonDocument = new HashMap<String, Object>();
		jsonDocument.put("title", title);
		jsonDocument.put("content", content);
		jsonDocument.put("postDate", postDate);
		jsonDocument.put("tags", tags);
		jsonDocument.put("author", author);
		return jsonDocument;
	}

	public static void getDocument(Client client, String index, String type, String id){
		client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
		//client.admin().cluster().health(new ClusterHealthRequest(index).waitForActiveShards(1)).actionGet();
		GetResponse getResponse = client.prepareGet(index, type, id).setRefresh(true).execute().actionGet();
		Map<String, Object> source = getResponse.getSource();

		System.out.println("------------------------------");
		System.out.println("Index: " + getResponse.getIndex());
		System.out.println("Type: " + getResponse.getType());
		System.out.println("Id: " + getResponse.getId());
		System.out.println("Version: " + getResponse.getVersion());
		System.out.println(source);
		System.out.println("------------------------------");
	}

	public static void updateDocument(Client client, String index, String type, 
			String id, String field, String newValue){

		Map<String, Object> updateObject = new HashMap<String, Object>();
		updateObject.put(field, newValue);

		client.prepareUpdate(index, type, id).setScript("ctx._source." + field + "=" + field, ScriptService.ScriptType.INLINE)
		.setRefresh(true)
		.setScriptParams(updateObject).execute().actionGet();
	}

	public static void updateDocument(Client client, String index, String type,
			String id, String field, String[] newValue){

		String tags = "";
		for(String tag :newValue)
			tags += tag + ", ";

		tags = tags.substring(0, tags.length() - 2);

		Map<String, Object> updateObject = new HashMap<String, Object>();
		updateObject.put(field, tags);

		client.prepareUpdate(index, type, id)
		.setScript("ctx._source." + field + "+=" + field, ScriptService.ScriptType.INLINE)
		.setScriptParams(updateObject).execute().actionGet();
	}

	public static void searchDocument(Client client, String index, String type,
			String field, String value){
		
		client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
		SearchResponse response = client.prepareSearch(index)
				.setTypes(type)
				.setSearchType(SearchType.QUERY_AND_FETCH)
			    .setQuery(QueryBuilders.matchQuery(field, value))
			    //.setQuery(QueryBuilders.termQuery(field, value))
				.setFrom(0).setSize(60).setExplain(true)
				.execute()
				.actionGet();

		SearchHit[] results = response.getHits().getHits();

		System.out.println("Current results: " + results.length);
		for (SearchHit hit : results) {
			System.out.println("------------------------------");
			Map<String,Object> result = hit.getSource();   
			System.out.println(result);
		}
	}

	public static void deleteDocument(Client client, String index, String type, String id){

		DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
		System.out.println("Information on the deleted document:");
		System.out.println("Index: " + response.getIndex());
		System.out.println("Type: " + response.getType());
		System.out.println("Id: " + response.getId());
		System.out.println("Version: " + response.getVersion());
	}

}
