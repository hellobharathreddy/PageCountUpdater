package com.projak.psb.searchutil;

import java.io.IOException;
import java.util.Iterator;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.pdfbox.pdmodel.PDDocument;

import com.filenet.api.collection.IndependentObjectSet;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.Connection;
import com.filenet.api.core.Document;
import com.filenet.api.core.Domain;
import com.filenet.api.core.Factory;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.property.Properties;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;
import com.filenet.api.util.UserContext;

public class FileNetUtils {
	
	final Logger logger = Logger.getLogger(FileNetUtils.class);
	
	public static Connection connection = null;
	public static Domain domain = null;
	public static ObjectStore objectStore = null;
	
	public Connection getCEConnection(){
		
		String uri = PropertyReader.getProperty("DMS_CE_URI");
		String userName = PropertyReader.getProperty("DMS_CE_USERNAME");
		String password=PropertyReader.getProperty("DMS_CE_PASSWORD");
		try {
			logger.info("<<< Getting the CE Connection >>>");
			
		    connection = Factory.Connection.getConnection(uri);
		    logger.debug("MTOM URL:: "+uri);
		    Subject subject = UserContext.createSubject(connection, userName, password, null);
		    logger.debug("USERNAME:: " + userName);
		    UserContext.get().pushSubject(subject);
		    logger.info("Connection was successfull to the:: " + connection.getURI());
		} catch (Exception e) {
			logger.error("Exception in getCEConnection ==> " + e.getMessage());
		}
		return connection;
	}
	
	public Domain getDomain(Connection connection){
		try {
			logger.info("<<< Getting the Domain >>>");
			if(connection != null) {
				domain = Factory.Domain.fetchInstance(connection, null, null);
			    logger.info("Domain Name:: " + domain.get_Name());
			} else {
				logger.info("Connection unsuccessfull!");
			}
		} catch (Exception e) {
			logger.error("Exception in getDomain ==> " + e.getMessage());
		}
		return domain;
	}
	
	public ObjectStore getObjectStore(Connection connection, Domain domain, String objectStoreName) {
		try {
			objectStore = Factory.ObjectStore.fetchInstance(domain, objectStoreName, null);
			logger.info("Retrieved ObjectStore Name:: "+objectStore.get_Name());
		} catch (Exception e) {
			logger.error("Exception in getObjectStore ==> " + e.getMessage());
		}
		return objectStore;
	}
	public void searchDocuments(ObjectStore objectStore){
		
		SearchScope search = new SearchScope(objectStore);
		SearchSQL sqlObject = new SearchSQL();
		//sqlObject.setSelectList("d.DocumentTitle, d.Id, d.MimeType, d.ContentElements,d.FN_PageCount");
		sqlObject.setSelectList(PropertyReader.getProperty("DMS_QUERY_SELECTLIST"));
		logger.info("SelectList Query:: " + PropertyReader.getProperty("DMS_QUERY_SELECTLIST"));
		//sqlObject.setMaxRecords(Integer.parseInt(PropertyReader.getProperty("DMS_QUERY_MAXRECORDS")));
		sqlObject.setFromClauseInitialValue(PropertyReader.getProperty("DMS_QUERY_FROMCLAUSE"), "d", true);
		logger.info("FromClause Value:: " + PropertyReader.getProperty("DMS_QUERY_FROMCLAUSE"));
		// Specify the WHERE clause using the setWhereClause method.
		//String whereClause = "d.DocumentTitle LIKE '%T%'";
		String whereClause = PropertyReader.getProperty("DMS_QUERY_WHERECLAUSE");
		logger.info("Where Clause:: " + PropertyReader.getProperty("DMS_QUERY_WHERECLAUSE"));
		sqlObject.setWhereClause(whereClause);
		// Check the SQL statement.  
		logger.info("SQL: " + sqlObject.toString());
		// Set the page size (Long) to use for a page of query result data. This value is passed 
		// in the pageSize parameter. If null, this defaults to the value of 
		// ServerCacheConfiguration.QueryPageDefaultSize.
		Integer myPageSize = new Integer(Integer.parseInt(PropertyReader.getProperty("DMS_QUERY_PAGESIZE")));
		logger.info("Page Size:: " + PropertyReader.getProperty("DMS_QUERY_PAGESIZE"));

		// Specify a property filter to use for the filter parameter, if needed. 
		// This can be null if you are not filtering properties.
//		PropertyFilter myFilter = new PropertyFilter();
//		int myFilterLevel = 1;
//		myFilter.setMaxRecursion(myFilterLevel);
//		myFilter.addIncludeType(new FilterElement(null, null, null, FilteredPropertyType.ANY, null)); 

		// Set the (Boolean) value for the continuable parameter. This indicates 
		// whether to iterate requests for subsequent pages of result data when the end of the 
		// first page of results is reached. If null or false, only a single page of results is 
		// returned.
		Boolean continuable = new Boolean(true);

		// Execute the fetchObjects method using the specified parameters.
		IndependentObjectSet myObjects = search.fetchObjects(sqlObject, myPageSize, null, continuable);
		logger.info("is query returned empty objects?: "+myObjects.isEmpty());
		
		Iterator<Document> myDocument = myObjects.iterator();
		while (myDocument.hasNext()) {
			Document document = (Document) myDocument.next();
			//logger.info("document: "+document.getProperties().getStringValue("DocumentTitle"));
			Properties properties = document.getProperties();
	        
	        PDDocument pdDocument = null;
	        int pageCount = 0;
	      //  System.out.println("doc.get_MimeType():" +document.get_MimeType());
	        try {
	        	if(document.get_MimeType().equalsIgnoreCase("application/pdf")){
	        		logger.info("::: Loading the PDF Document :::");
	        		pdDocument = PDDocument.load(document.accessContentStream(0));
	                pageCount = pdDocument.getNumberOfPages();
	                logger.info("Number of Pages: " + pageCount);
	                properties.putValue("FN_PageCount", pageCount);
	                document.save(RefreshMode.NO_REFRESH);
	                logger.info("Document saved successfully!");
	        	}
	            
	        }catch(Exception e){
	        	logger.error("Exception ==> " + e.getMessage());
	        } finally {
	            if (pdDocument != null) {
	                try {
						pdDocument.close();
						logger.info("::: Document Closed :::");
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	                logger.info("PDF closed!");
	            }
	        }
	        
		}
	}
	public static void main(String[] args) {
		
		PropertyConfigurator.configure("D:\\PageCountUpdater\\configurations\\log4j.properties");
		final Logger logger = Logger.getLogger(FileNetUtils.class);
		
		FileNetUtils utils = new FileNetUtils();
		Connection conn = utils.getCEConnection();
		try {
			if(conn != null) {
				Domain dom = utils.getDomain(conn);
				String[] osNames = PropertyReader.getProperty("DMS_OSNames").split(",");
				logger.info("OS Names count:: "+osNames.length);
				for (String objStore : osNames) {
					logger.info("Passing ObjectStore Name:: "+objStore);
					ObjectStore os = utils.getObjectStore(conn, dom,objStore);
					utils.searchDocuments(os);
				}
			}else {
				logger.info("::: CE Connection not successful :::");
			}
		} catch(Exception e) {
			logger.error("Exception in main() ==> " + e.getMessage());
		}		
	}
}
