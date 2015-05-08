package com.test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;




public class TestDownloadXml {

	public static final String UNKNOWN = "UNKNOWN";

	public static void main(String[] args) throws Exception{
		getQuestionMapForTestId("13252698044100385 ");
	}

	public static List<Map<String, Object>> getQuestionMapForTestId( String testId) throws Exception{
		List<Map<String, Object>> xmlMapList = null;
		try{
			if(testId == null || testId.length()==0){
				throw new Exception("invalid Testid : "+testId);
			}
			testId = testId.trim();
			StringBuilder url = new StringBuilder("http://connectdev5.mheducation.com/ezto/hm.tpx?")
								.append("todo=picker_poolInfo&")
								.append("ezid=").append(testId).append("&")
								.append("uid=1000166962&")
								.append("key=95d52fe518b0112e2902b5c62412fbb6");
			//System.out.println(" url : "+url);
			String postRest = postREST(url.toString());
			//System.out.println("postRest : "+postRest);
			xmlMapList = getXMLElement(postRest);
			System.out.println(" xmlMapList : "+xmlMapList);
		}catch(Exception ex){
			System.out.println(" Problem in getting question metadata values for testid : "+testId);
		}
		return xmlMapList;
	}

	private static String postREST(String theURL) throws Exception{
		String theResults = "";
		try{
			URL url = new URL(theURL);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setRequestProperty("Content-Type", "text/xml");
			connection.setDoOutput(true);
			connection.setReadTimeout(5*60*1000);
			connection.connect();

			// read in the result
			BufferedInputStream classIn = new BufferedInputStream(connection.getInputStream());
			long length = connection.getContentLength();
			if (length < 0)
				length = classIn.available(); // sometimes the wrong number is
			// returned, try this method

			if (length == -1){
				classIn.close();
				System.out.println("restServices.postREST: Exception - no response from Connect ");
				return theResults;
			}else{
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				int result = classIn.read();
				while(result != -1) {
					byte b = (byte)result;
					bos.write(b);
					result = classIn.read();
				}        
				theResults = bos.toString();
				classIn.close();
				bos.close();
			}
		}
		catch (Exception x){
			System.out.println("restServices.postREST: Exception - general exception on post url: " + theURL);
			x.printStackTrace();
			throw x;
		}
		return theResults;
	}


	private static List<Map<String, Object>> getXMLElement( String input ) throws Exception{
		String points = UNKNOWN;
		String actualType = UNKNOWN;
		String qid = UNKNOWN;
		String title = UNKNOWN;
		String displayType =UNKNOWN;
		String scoring =UNKNOWN;
		List<Map<String, Object>> xmlMapList = new ArrayList<Map<String, Object>>();
		
		if(input == null || ("").equals(input)){
			return xmlMapList;
		}
		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document document = builder.parse(new ByteArrayInputStream(input.getBytes()));
		NodeList nodeList = document.getDocumentElement().getChildNodes();

		for(int i =0 ; i< nodeList.getLength(); i++){
			Map<String, Object> nodeMap = new HashMap<String, Object>();
			Node node = nodeList.item(i);
			if(node.getNodeType() == Node.ELEMENT_NODE){
				if("ezid".equals(node.getNodeName())){
					//System.out.println(node.getTextContent());
					nodeMap.put("ezid", node.getTextContent());
				}
				if("question".equals(node.getNodeName())){
					Element elem = (Element) node;

					if(elem !=null && elem.getElementsByTagName("qid") != null && elem.getElementsByTagName("qid").item(0) != null &&
							elem.getElementsByTagName("qid").item(0).getChildNodes() != null && elem.getElementsByTagName("qid").item(0).getChildNodes().item(0) != null){
						qid = elem.getElementsByTagName("qid").item(0).getChildNodes().item(0).getNodeValue();
					}

					if(elem !=null && elem.getElementsByTagName("title") != null && elem.getElementsByTagName("title").item(0) != null &&
							elem.getElementsByTagName("title").item(0).getChildNodes() != null && elem.getElementsByTagName("title").item(0).getChildNodes().item(0) != null){
						title = elem.getElementsByTagName("title").item(0).getChildNodes().item(0).getNodeValue();
					}

					if(elem !=null && elem.getElementsByTagName("displayType") != null && elem.getElementsByTagName("displayType").item(0) != null &&
							elem.getElementsByTagName("displayType").item(0).getChildNodes() != null && elem.getElementsByTagName("displayType").item(0).getChildNodes().item(0) != null){
						displayType = elem.getElementsByTagName("displayType").item(0).getChildNodes().item(0).getNodeValue();
					}

					if(elem !=null && elem.getElementsByTagName("actualType") != null && elem.getElementsByTagName("actualType").item(0) != null &&
							elem.getElementsByTagName("actualType").item(0).getChildNodes() != null && elem.getElementsByTagName("actualType").item(0).getChildNodes().item(0) != null){
						actualType = elem.getElementsByTagName("actualType").item(0).getChildNodes().item(0).getNodeValue();
					}

					if(elem !=null && elem.getElementsByTagName("points") != null && elem.getElementsByTagName("points").item(0) != null &&
							elem.getElementsByTagName("points").item(0).getChildNodes() != null && elem.getElementsByTagName("points").item(0).getChildNodes().item(0) != null){
						points = elem.getElementsByTagName("points").item(0).getChildNodes().item(0).getNodeValue();
					}

					if(elem !=null && elem.getElementsByTagName("scoring") != null && elem.getElementsByTagName("scoring").item(0) != null &&
							elem.getElementsByTagName("scoring").item(0).getChildNodes() != null && elem.getElementsByTagName("scoring").item(0).getChildNodes().item(0) != null){
						scoring = elem.getElementsByTagName("scoring").item(0).getChildNodes().item(0).getNodeValue();
					}

					nodeMap.put("qid", qid);
					nodeMap.put("title", title);
					nodeMap.put("displayType", displayType);
					nodeMap.put("actualType", actualType);
					nodeMap.put("points", points);
					nodeMap.put("scoring", scoring);
					/*System.out.println(" ==============================Start========================================== ");
					System.out.println(" qid : "+qid);
					System.out.println(" title : "+title);
					System.out.println(" displayType : "+displayType);
					System.out.println(" actualType : "+actualType);
					System.out.println(" points : "+points);
					System.out.println(" scoring : "+scoring);
					System.out.println(" ==============================End========================================== ");*/
					xmlMapList.add(nodeMap);
				}
			}
		}
		return xmlMapList;
	}

}
