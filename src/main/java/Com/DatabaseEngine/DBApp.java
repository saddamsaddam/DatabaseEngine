package Com.DatabaseEngine;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util. LinkedHashMap;
import java.util.Map.Entry;
import java.util.Vector;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;

public class DBApp {
    Vector<String[]> vectordata = new Vector<String[]>();

	public static int pagerownumber=2;

	public DBApp() {
		super();
		
	}

	 public void createTable(String strTableName, String strClusteringKeyColumn,  LinkedHashMap<String,String> htblColNameType,  LinkedHashMap<String,String> htblColNameMin,  LinkedHashMap<String,String> htblColNameMax ) throws DBAppException, IOException 
			 {	
		            vectordata = new Vector<String[]>(); 
		            String[] header = new String[htblColNameType.size()+2];
			        String[] data =new String[htblColNameType.size()+2];
			       
			        //access all key and value
			        int i=0;
		        for (Entry<String, String> e : htblColNameType.entrySet())
		        {
		         // System.out.println(e.getKey() + "->" + e.getValue());
	                 header[i]=e.getKey();
	                 data[i]=e.getValue();
	                 i++;
		        }
		        header[header.length-1]="Max";	
		        header[header.length-2]="Min";
		        
		        data[header.length-1]=htblColNameMax.toString();	
		        data[header.length-2]=htblColNameMin.toString();
		        vectordata.add(header);
		        vectordata.add(data);
		        CSVWriter writer = new CSVWriter(new FileWriter("src/main/resources/"+strTableName+".csv"));
		        writer.writeAll(vectordata);
		        System.out.println(" Table Created successfully.");

		        writer.close();
		        
		        
			 }
	 public void insertIntoTable(String strTableName,   LinkedHashMap<String,String> htblColNameValue)  throws DBAppException, IOException, CsvException
			 {
	        // read the existing data from the CSV file into a List
		    String path;
		    CSVReader reader ;
		    int rownumber;
		    int filenumber=numberoffile(strTableName,"src/main/resources/");
		    if(filenumber==1)
		    {
		    	rownumber= numberofrow("src/main/resources/"+strTableName+".csv");
		    	reader = new CSVReader(new FileReader("src/main/resources/"+strTableName+".csv"));
		    	path="src/main/resources/"+strTableName+".csv";
		    }
		    else
		    {
		    	rownumber= numberofrow("src/main/resources/"+strTableName+""+(filenumber-1)+".csv");
		    	reader = new CSVReader(new FileReader("src/main/resources/"+strTableName+""+(filenumber-1)+".csv"));
		    	path="src/main/resources/"+strTableName+""+(filenumber-1)+".csv";
		    }
	        Vector<String[]> existingData = new Vector<String[]>();
	        String[] record;
	        while ((record = reader.readNext()) != null) {
	            // Add each line to the vector
	        	existingData.add(record);
	        }
	        reader.close();
	        CSVWriter writer;
	     // create a new instance of CSVWriter and specify the path to your CSV file
		   //  writer = new CSVWriter(new FileWriter("src/main/resources/"+strTableName+".csv"));
		     if(rownumber>(pagerownumber-1))
		     {
				    vectordata = new Vector<String[]>();
			     writer = new CSVWriter(new FileWriter("src/main/resources/"+strTableName+""+filenumber+".csv"));
			     String[] row;
			     String[] data=new String[htblColNameValue.size()];
			     String[] header=tableheader(path);
                   vectordata.add(header);
			     String[] datatype=tabledatatype(path);
                    vectordata.add(datatype);
				   int i=0;
				  for (Entry<String, String> e : htblColNameValue.entrySet())
			        {
			         // System.out.println(e.getKey() + "->" + e.getValue());
			          data[i]= e.getValue();
		                i++;
			        }


		        // add the new row of data to the list or array of existing data

		        // write the combined data (existing data and new data) to the CSV file
				  vectordata.add(data);
				  writer.writeAll(vectordata);
		        

		     }
		     else
		     {
		    	 if(filenumber==1)
		    	 {
				     writer = new CSVWriter(new FileWriter("src/main/resources/"+strTableName+".csv"));
				     String[] data=new String[htblColNameValue.size()];
					   int i=0;
					  for (Entry<String, String> e : htblColNameValue.entrySet())
				        {
				         // System.out.println(e.getKey() + "->" + e.getValue());
				          data[i]= e.getValue();
			                i++;
				        }


			        // add the new row of data to the list or array of existing data
			        existingData.add(data);

			        // write the combined data (existing data and new data) to the CSV file
			        writer.writeAll(existingData);
 
		    	 }
		    	 else
		    	 {
					 writer = new CSVWriter(new FileWriter("src/main/resources/"+strTableName+""+(filenumber-1)+".csv"));
					 String[] data=new String[htblColNameValue.size()];
					   int i=0;
					  for (Entry<String, String> e : htblColNameValue.entrySet())
				        {
				         // System.out.println(e.getKey() + "->" + e.getValue());
				          data[i]= e.getValue();
			                i++;
				        }


			        // add the new row of data to the list or array of existing data
			        existingData.add(data);

			        // write the combined data (existing data and new data) to the CSV file
			        writer.writeAll(existingData);
 
		    	 }

 
		     }
	          
	        System.out.println(" Inserted successfully.");


	        // close the CSVWriter instance
	        writer.close();
		 
		     //System.out.println(strTableName);
			 }
	 public void updateTable(String strTableName,  String strClusteringKeyValue,  LinkedHashMap<String,String> htblColNameValue ) throws DBAppException, IOException, CsvValidationException
			{
		 int id=Integer.parseInt(strClusteringKeyValue);
		 CSVReader reader;
		 String path;

		 if(id<pagerownumber)
		 {
			 path="src/main/resources/"+strTableName+".csv";
			 reader = new CSVReader(new FileReader(path));

		 }
		 else
		 {
			 int dividend = id+1;
			 int divisor = pagerownumber;

			 double result = (double)dividend / (double)divisor; // Cast to double to perform floating-point division
			 int upperValue = (int)Math.ceil(result);
			 path="src/main/resources/"+strTableName+""+(upperValue-1)+".csv";
			 reader = new CSVReader(new FileReader(path));
 
		 }
		  

		
		 CSVWriter writer = new CSVWriter(new FileWriter("src/main/resources/"+strTableName+"temp.csv"));
		 //String[] headers = reader.readNext(); // skip header row
		

         String[] row;
		    vectordata = new Vector<String[]>();

         while ((row = reader.readNext()) != null) {
        	 if(row[0].equals(strClusteringKeyValue))
        	 {
        		 System.out.println(row[4]);
        		 String[] data=new String[htblColNameValue.size()+1]; 
        		 data[0]=strClusteringKeyValue;
        		 int i=1;
   			  for (Entry<String, String> e : htblColNameValue.entrySet())
   		        {
   		         // System.out.println(e.getKey() + "->" + e.getValue());
   		          data[i]= e.getValue();
   	                i++;
   		        }
        	 
              vectordata.add(data);
    
        	 }
        	 else
        	 {
        	   String[] data=new String[htblColNameValue.size()+1]; 
	        	 for(int i=0;i<htblColNameValue.size()+1;i++)
	        	 {
	        		 data[i]=row[i];
	        	 }
 
        		vectordata.add(data); 
        	 }
        	

            
           }
         writer.writeAll(vectordata);
         reader.close();
	     writer.close();
	     //delete main file
	     File tempFile = new File(path);
	     tempFile.delete();
	     
	  // Create a File object for the original file
	     File originalFile = new File(path);
	     // Create a File object for the temporary file
	     tempFile = new File("src/main/resources/"+strTableName+"temp.csv");

	     // Rename the temporary file to the original file
	     boolean rename = tempFile.renameTo(originalFile);

	     if (rename) {
	         System.out.println("id ->"+strClusteringKeyValue+"  updated successfully.");
	     } else {
	         System.out.println("id ->"+strClusteringKeyValue+"  Failed to update .");
	     }

			}
	 public void deleteFromTable(String strTableName,  LinkedHashMap<String,String> htblColNameValue) throws DBAppException, IOException, CsvValidationException 
			 {
		 
		

		 //String[] headers = reader.readNext(); // skip header row
 
		 for (Entry<String, String> f : htblColNameValue.entrySet())
	        {
			    vectordata = new Vector<String[]>();

			 CSVWriter writer = new CSVWriter(new FileWriter("src/main/resources/"+strTableName+"temp.csv"));
			 
			 int id=Integer.parseInt(f.getKey());
			 CSVReader reader;
			 String path;

			 if(id<pagerownumber)
			 {
				 path="src/main/resources/"+strTableName+".csv";
				 reader = new CSVReader(new FileReader(path));

			 }
			 else
			 {
				 int dividend = id+1;
				 int divisor = pagerownumber;

				 double result = (double)dividend / (double)divisor; // Cast to double to perform floating-point division
				 int upperValue = (int)Math.ceil(result);
				 path="src/main/resources/"+strTableName+""+(upperValue-1)+".csv";
				 reader = new CSVReader(new FileReader(path));
	 
			 }
			  
			 
			
         String[] row;
         while ((row = reader.readNext()) != null) {
        	 
        	 if(!row[0].equals( f.getKey()))
        	 {
        		 String[] data=new String[row.length]; 
	        	 for(int i=0;i<row.length;i++)
	        	 {
	        		 data[i]=row[i];
	        	 }
 
        	  vectordata.add(data);
        		 
        	 }
        	
            
           }

         reader.close();
         writer.writeAll(vectordata);
	     writer.close();
	     //delete main file
	     File tempFile = new File(path);
	     tempFile.delete();
	     
	  // Create a File object for the original file
	     File originalFile = new File(path);
	     // Create a File object for the temporary file
	     tempFile = new File("src/main/resources/"+strTableName+"temp.csv");

	     // Rename the temporary file to the original file
	     boolean rename = tempFile.renameTo(originalFile);
		
			     if (rename) {
			         System.out.println(f.getValue()+"-> "+f.getKey()+"   deleted successfully.");
			     } else {
			         System.out.println(f.getValue()+"-> "+f.getKey()+"  Failed to delete .");
			     }
			     //delete empty file
        		 int rownumber=numberofrow(path);
        		 if(rownumber==0)
        		 {
        			 tempFile = new File(path);
        		     tempFile.delete(); 
        		 }

			 }
        

			 }
	 
	 public String[] tableheader(String path) throws CsvValidationException, IOException
	 {
		 CSVReader reader = new CSVReader(new FileReader(path));
		 int count=0;
		 String[] row;
		 int lng=0;
         while ((row = reader.readNext()) != null) {
        	 if(count==0)
        	 {
        		lng=row.length; 
        		System.out.println(row[4]);
        		break;
        	 }
     		count++;

           }
         reader.close();
         CSVReader reader1 = new CSVReader(new FileReader(path));
		 String[]data=new String[lng];
          count=0;
         while ((row = reader1.readNext()) != null) {
        	 if(count==0)
        	 {
        		 for(int i=0;i<lng;i++)
        		 {
        			data[i]=row[i]; 
        		 }
        		 break;
        	 }
        	 count++;
           }
         reader1.close();
		 return data;
	 }
	 
	 public String[] tabledatatype(String path) throws CsvValidationException, IOException
	 {
		 CSVReader reader = new CSVReader(new FileReader(path));
		 int count=0;
		 String[] row;
		 int lng=0;
         while ((row = reader.readNext()) != null) {
        	 if(count==1)
        	 {
        		lng=row.length; 
        		System.out.println(row[4]);
        		break;
        	 }
     		count++;

           }
         reader.close();
         CSVReader reader1 = new CSVReader(new FileReader(path));
		 String[]data=new String[lng];
          count=0;
         while ((row = reader1.readNext()) != null) {
        	 if(count==1)
        	 {
        		 for(int i=0;i<lng;i++)
        		 {
        			data[i]=row[i]; 
        		 }
        		 break;
        	 }
        	 count++;
           }
         reader1.close();
		 return data;
	 }
	
	 public int  numberofrow(String path) throws CsvValidationException, IOException
	 {
		 CSVReader reader = new CSVReader(new FileReader(path));
		 reader.readNext();
		 reader.readNext();
		 int count=0;
		 String[] row;
         while ((row = reader.readNext()) != null) {
        	count++;
           }
         reader.close();
		 return count;
	 }
	public int numberoffile(String filname,String path)
	{
	        File folder = new File(path);
	        File[] listOfFiles = folder.listFiles();
	        int count=0;

	        for (File file : listOfFiles) {
	            if (file.isFile() && file.getName().endsWith(".csv")&& file.getName().startsWith(filname)) {
	            	count++;
	            }
	        }
		
		return count;
	}
	public static void main(String[] args) throws DBAppException, IOException, CsvException {
		System.out.println("Database Engine");
		
		//create table
		String strTableName = "Aiboot"; 
		DBApp dbApp = new DBApp( ); 
		 LinkedHashMap htblColNameType = new  LinkedHashMap( ); 
		htblColNameType.put("id", "java.lang.Integer"); 
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("address", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
		htblColNameType.put("date", "java.util.Date");
		 LinkedHashMap htblColNameMin = new  LinkedHashMap( );
		htblColNameMin.put("id","0");
		htblColNameMin.put("gpa","0");
		 LinkedHashMap htblColNameMax = new  LinkedHashMap( );
		htblColNameMax.put("id","1000000");
		htblColNameMax.put("gpa","4");
		
		//dbApp.createTable( strTableName, "true", htblColNameType,htblColNameMin,htblColNameMax); 
		
		//insert data
		 LinkedHashMap htblColNameValue = new  LinkedHashMap( ); 
		htblColNameValue.put("id","0"); 
		htblColNameValue.put("name","Ahmed Noor");
		htblColNameValue.put("address","Dhaka");
		htblColNameValue.put("gpa", ".9" ); 
		htblColNameValue.put("date", "2020-03-23" ); 
		//dbApp.insertIntoTable( strTableName , htblColNameValue ); 
		htblColNameValue.clear( ); 
		htblColNameValue.put("id","1"); 
		htblColNameValue.put("name","Ahmed Noor1");
		htblColNameValue.put("address","borishal");
		htblColNameValue.put("gpa", ".9" ); 
		htblColNameValue.put("date", "2020-03-23" ); 
		//dbApp.insertIntoTable( strTableName , htblColNameValue ); 
		htblColNameValue.clear( ); 
		htblColNameValue.put("id","2"); 
		htblColNameValue.put("name","Ahmed Noor2");
		htblColNameValue.put("address","borisha2");
		htblColNameValue.put("gpa", ".9" ); 
		htblColNameValue.put("date", "2020-03-23" ); 
		//dbApp.insertIntoTable( strTableName , htblColNameValue ); 
		htblColNameValue.clear( ); 
		htblColNameValue.put("id","3"); 
		htblColNameValue.put("name","Ahmed Noor2");
		htblColNameValue.put("address","borisha2");
		htblColNameValue.put("gpa", ".9" );
		htblColNameValue.put("date", "2020-03-23" ); 
		//dbApp.insertIntoTable( strTableName , htblColNameValue ); 
		htblColNameValue.clear( ); 
		htblColNameValue.put("id","4"); 
		htblColNameValue.put("name","Ahmed Noor2");
		htblColNameValue.put("address","borisha2");
		htblColNameValue.put("gpa", ".9" );
		htblColNameValue.put("date", "2020-03-23" ); 
		dbApp.insertIntoTable( strTableName , htblColNameValue ); 
		 		
		//update
		htblColNameValue.clear( ); 
		htblColNameValue.put("name","Ahmed Noorupdate");
		htblColNameValue.put("address","Dhakatoupdate");
		htblColNameValue.put("gpa", ".94" );
		htblColNameValue.put("date", "03-23-2020" ); 
		//dbApp.updateTable(strTableName, "1", htblColNameValue);
		
	   //delete 
		htblColNameValue.clear( ); 
		htblColNameValue.put("4","id"); 
		//htblColNameValue.put("1","id");

		//dbApp.deleteFromTable(strTableName, htblColNameValue);


	}

}
