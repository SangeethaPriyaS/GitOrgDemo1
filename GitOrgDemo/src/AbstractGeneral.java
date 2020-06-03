/* VERSION:  |11.01.0128| TIME 20-May-2014 07:49:39 FILE: AbstractGeneral.java DEV:9296 REL:9304 */

/* ***** Development File Details:$Rev: 9296 $, $Author: sangeetha $, $Date: 2014-05-19 10:06:50 -0400 (Mon, 19 May 2014) $ ***** Added by Program */
/* VERSION: 10.5.0.1159 TIME: 25-Oct-2011 07:52:35 FILE: AbstractGeneral.java */

/*
 * FileName 	: AbstractGeneral.java
 * Author   	: E1212
 * Created Date : Aug 8, 2007
 */
package epm;

import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Hashtable;
import java.util.Vector;
import java.util.Properties;


import epm.dbutil.DbManager;
import epm.util.AppSession;
import epm.util.Constant;
import epm.util.GenericFactory;
import epm.util.Logger;
import epm.util.UserRequest;
import epm.util.STKGeneral;
import epm.util.TableConstants;
/** 
 * This Class contains General functions widely used in New Abstract Module.
 */
public class AbstractGeneral 
{

public String m_SVNDevelopmentDetails = "Development File Details:$Rev: 9296 $, $Author: sangeetha $, $Date: 2014-05-19 10:06:50 -0400 (Mon, 19 May 2014) $";
 public String _VERSION_ = "|VERSION| |11.01.0128|TIME|20-May-2014 07:49:39|DEV|9296|REL|9304";


    private static final String	CLASS_NAME = "AbstractGeneral";
    private static final String ORACLE_DRIVER	= "oracle.jdbc.driver.OracleDriver";
    private static final String USER			= "user";
	private static final String PASSWORD		= "password";
   
    private String syskey1 = "";
    private String poolName = "";

    private String errLogPath = "";
    public AbstractGeneral()
    {
        
    }
	public AbstractGeneral(String aSyskey1, String aPoolName)
	{
	    syskey1 = aSyskey1;
	    poolName = aPoolName;
	}
	
	/**
	 * This method is to get the current version for each tenant
	 * @param sysKey1	Syskey
	 * @param poolName	Poolname
	 * @return Hashtable
	 * @throws Exception 
	 */
	public static Hashtable getCurrentVersion(String sysKey1,String poolName) throws Exception
	{
		Hashtable curVersionHash = new Hashtable();
		StringBuffer sql = new StringBuffer(); 
		sql.append(" SELECT PROPID||'-'||TENID||'-'||TENSEQNO PROPTEN,TO_NUMBER(VERSIONNO),DESCRIPTION,CURR_VER FROM ").append( TableConstants.TABLE_ABWPTVER);
		sql.append(" WHERE SYS_KEY1 = '").append(sysKey1).append("'   AND CURR_VER='").append(Constant.CHAR_YES).append("'"); 
		// Logger.log(new UserRequest(),CLASS_NAME, "@@getCurrentVersion-sql : "  ,sql, Logger.INFO, Logger.LOG_ABSTRACT);		 
		 curVersionHash = DbManager.getQueryResultHash(new Vector(), sql.toString(),"PROPTEN", poolName);		 
		 //Logger.log(new UserRequest(),CLASS_NAME, "@@getCurrentVersion-curVersionHash : "  ,curVersionHash, Logger.INFO, Logger.LOG_ABSTRACT);		
		return curVersionHash;		
	}
	/**
	 * This method is to get the max version for each tenant
	 * @param sysKey1	Syskey
	 * @param poolName	Poolname
	 * @return Hashtable
	 * @throws Exception 
	 */
	public static Hashtable getMaxVersion(String sysKey1,String poolName) throws Exception
	{
		Hashtable maxVersionHash = new Hashtable();
		StringBuffer sql = new StringBuffer(); 
		sql.append("SELECT PROPID || '-' || TENID || '-' || TENSEQNO PROPTEN, TO_NUMBER (MAX (VERSIONNO)) FROM ").append( TableConstants.TABLE_ABWPTVER);
		sql.append(" WHERE SYS_KEY1 = '").append(sysKey1).append("' GROUP BY PROPID || '-' || TENID || '-' || TENSEQNO ORDER BY PROPTEN"); 
		 Logger.log(new UserRequest(),CLASS_NAME, "@@getMaxVersion-sql : "  ,sql, Logger.INFO, Logger.LOG_ABSTRACT);		 
		maxVersionHash = DbManager.getQueryResultHash(new Vector(), sql.toString(),"PROPTEN", poolName);		 
		 //Logger.log(new UserRequest(),CLASS_NAME, "@@getMaxVersion: maxVersionHash.. "  ,maxVersionHash, Logger.INFO, Logger.LOG_ABSTRACT);		
		return maxVersionHash;		
	}
	/**
	 * This method returns the current version number for a tenant
	 * @param versionHash	Version collection
	 * @param propId Property id
	 * @param tenId  Tenant id
	 * @param tenSeq Tenant Sequence
	 * @return int
	 * @throws Exception
	 */
	public static int getVersionNo(Hashtable versionHash,String propId,String tenId,String tenSeq) throws Exception
	{
		float versionNo = 0.0f;
		int verNo = 0;
		String propTen = STKGeneral.getPaddedProperty(propId)+'-'+STKGeneral.getPaddedTenant(tenId)+'-'+tenSeq;
		if(versionHash != null && versionHash.size()>0)
		{
			if(versionHash.get(propTen) != null)
			{
				Vector versionVec = (Vector)versionHash.get(propTen);
				verNo = STKGeneral.getInteger(((String)versionVec.elementAt(1)).trim());	
				/*versionNo = STKGeneral.getFloat(((String)versionVec.elementAt(1)).trim());		
				verNo = Math.round(versionNo);	*/			
			}
		}
		
		return verNo;
	}
	
	/**
	 * This method returns the current version number for a tenant
	 * @param versionHash	Version collection
	 * @param propId Property id
	 * @param tenId  Tenant id
	 * @param tenSeq Tenant Sequence
	 * @return int
	 * @throws Exception
	 */
	public static int getVersionNo(Hashtable versionHash,String propId,String tenId,String tenSeq, String absClass) throws Exception
	{
		float versionNo = 0.0f;
		int verNo = 0;
		
		if(absClass.equalsIgnoreCase(Constant.PROP_ABSTRACT))
	    {		         
		     tenId = Constant.EMPTY_TENANT;
		     tenSeq = Constant.EMPTY_TENSEQ;		 	    
	    }
	    else if(absClass.equalsIgnoreCase(Constant.UNIT_ABSTRACT))
	    {	           
           tenSeq= Constant.EMPTY_TENSEQ;
	    }	    

		String propTen = STKGeneral.getPaddedProperty(propId)+'-'+STKGeneral.getPaddedTenant(tenId)+'-'+tenSeq;
		if(versionHash != null && versionHash.size()>0)
		{
			if(versionHash.get(propTen) != null)
			{
				Vector versionVec = (Vector)versionHash.get(propTen);
				verNo = STKGeneral.getInteger(((String)versionVec.elementAt(1)).trim());	
				/*versionNo = STKGeneral.getFloat(((String)versionVec.elementAt(1)).trim());		
				verNo = Math.round(versionNo);	*/			
			}
		}
		
		return verNo;
	}
	/**
	 * This method returns the current version number for a tenant
	 * @param versionHash Version collection
	 * @param propId Property id
	 * @param tenId	Tenant id
	 * @param tenSeq Tenant sequence
	 * @return Vector
	 * @throws Exception
	 */
	public static Vector getVersionNoDesc(Hashtable versionHash,String propId,String tenId,String tenSeq) throws Exception
	{		
		Vector versionVec = null; 
		String propTen = propId+'-'+tenId+'-'+tenSeq;
		
		if(versionHash != null && versionHash.size()>0)
		{
			if(versionHash.get(propTen) != null)
			{
				versionVec = (Vector)versionHash.get(propTen);							
			}
		}
		return versionVec;
	}
	/**
	 * This method is to get the Previous version for each tenant
	 * @param sysKey1 Syskey
	 * @param poolName Poolname
	 * @return Hashtable
	 * @throws Exception
	 */
	public static Hashtable getPreviousVersion(String sysKey1,String poolName) throws Exception
	{
		Hashtable prevVersionHash = new Hashtable();
		StringBuffer sql = new StringBuffer(); 
		sql.append(" SELECT A.PROPID||'-'||A.TENID||'-'||A.TENSEQNO PROPTEN, MAX(TO_NUMBER(A.VERSIONNO)) from ").append(TableConstants.TABLE_ABWPTVER).append(" A ,").append(TableConstants.TABLE_ABWPTVER).append(" B ");
		sql.append(" WHERE TO_NUMBER(A.VERSIONNO) < TO_NUMBER(B.VERSIONNO) AND B.CURR_VER='").append(Constant.CHAR_YES).append("'");
		sql.append(" AND A.PROPID = B.PROPID AND A.TENID = B.TENID AND A.TENSEQNO = B.TENSEQNO GROUP BY A.PROPID||'-'||A.TENID||'-'||A.TENSEQNO");
		//Logger.log(new UserRequest(),CLASS_NAME, "@@getPreviousVersion-sql : "  ,sql, Logger.INFO, Logger.LOG_ABSTRACT);		 
		prevVersionHash = DbManager.getQueryResultHash(new Vector(), sql.toString(),"PROPTEN", poolName);		 
		//Logger.log(new UserRequest(),CLASS_NAME, "@@getPreviousVersion-prevVersionHash : "  ,prevVersionHash, Logger.INFO, Logger.LOG_ABSTRACT);		
		return prevVersionHash;		
	}
	/**
	 * This method returns collection of tenant with curversion in the custom table
	 * @param sysKey1	Syskey
	 * @param poolName Poolname
	 * @param tblName	Table name
	 * @return Vector
	 * @throws Exception
	 */
	public static Vector getTenantsWithCurVer(String sysKey1,String poolName,String tblName) throws Exception
	{
		Vector tenants = new Vector();
		StringBuffer sql = new StringBuffer(); 
		sql.append(" SELECT DISTINCT T.PROPID||'-'||T.TENID||'-'||T.TENSEQNO  TENANTS, T.PROPID, T.TENID, T.TENSEQNO,TO_NUMBER(VER.VERSIONNO) FROM ");
		sql.append(tblName.trim()).append(" T ,").append(TableConstants.TABLE_ABWPTVER).append(" VER");
		sql.append(" WHERE  VER.SYS_KEY1 = '").append(sysKey1).append("' AND VER.CURR_VER='").append(Constant.CHAR_YES).append("'");
		sql.append(" AND  T.PROPID = VER.PROPID AND T.TENID = VER.TENID ");	
		//Logger.log(new UserRequest(),CLASS_NAME, "@@getCurrentVersion-sql : "  ,sql, Logger.INFO, Logger.LOG_ABSTRACT);		 
		tenants = DbManager.getQueryResult(new Vector(), sql.toString(), poolName);		 
		//Logger.log(new UserRequest(),CLASS_NAME, "@@getTenantsWithCurVer-tenants : "  ,tenants, Logger.INFO, Logger.LOG_ABSTRACT);		
		return tenants;		
	}
	/**
	 * This method returns collection of tenant with curversion in the ABWPTVER
	 * @param sysKey1	Syskey
	 * @param poolName Poolname
	 * @param tblName	Table name
	 * @return Vector
	 * @throws Exception
	 */
	public static Vector getTenantsWithCurVer(String sysKey1,String poolName) throws Exception
	{
		Vector tenants = new Vector();
		StringBuffer sql = new StringBuffer(); 
		sql.append(" SELECT DISTINCT PROPID||'-'||TENID||'-'||TENSEQNO  TENANTS, PROPID, TENID, TENSEQNO,TO_NUMBER(VERSIONNO) FROM ");
		sql.append(TableConstants.TABLE_ABWPTVER);
		sql.append(" WHERE  SYS_KEY1 = '").append(sysKey1).append("' AND CURR_VER='").append(Constant.CHAR_YES).append("'");
		sql.append(" ORDER BY TENANTS");
	//	Logger.log(new UserRequest(),CLASS_NAME, "@@getTenantsWithCurVer-tenants : -sql : "  ,sql, Logger.FATAL, Logger.LOG_ABSTRACT);		 
		tenants = DbManager.getQueryResult(new Vector(), sql.toString(), poolName);		 
		//Logger.log(new UserRequest(),CLASS_NAME, "@@getTenantsWithCurVer-tenants : "  ,tenants, Logger.FATAL, Logger.LOG_ABSTRACT);		
		return tenants;		
	}
	/**
	 * This method is used to get Version description
	 * @param sysKey1	Syskey
	 * @param poolName	Poolname
	 * @param versionNo	Version no
	 * @param propId	Property id
	 * @param tenId		Tenant id
	 * @param tenSeqNo	Tenant sequence no
	 * @return	Vector
	 * @throws Exception
	 */
	public static Vector getVersionDesc(String sysKey1,String poolName,String versionNo,String propId,String tenId,String tenSeqNo,String absClass) throws Exception
	{		
		Vector versionVec = new Vector();
		StringBuffer sql = new StringBuffer(); 
		if(absClass.equalsIgnoreCase(Constant.PROP_ABSTRACT))
	    {		         
		     tenId = Constant.EMPTY_TENANT;
		     tenSeqNo = Constant.EMPTY_TENSEQ;			    
	    }
	    else if(absClass.equalsIgnoreCase(Constant.UNIT_ABSTRACT))
	    {	           
           tenSeqNo = Constant.EMPTY_TENSEQ;
	    }	
		sql.append(" SELECT TO_NUMBER(VERSIONNO),DESCRIPTION,CURR_VER FROM ").append( TableConstants.TABLE_ABWPTVER);
		sql.append(" WHERE SYS_KEY1 = '").append(sysKey1).append("' AND TO_NUMBER(VERSIONNO) ='").append(versionNo).append("'"); 
		sql.append(" AND PROPID = '").append(STKGeneral.getPaddedProperty(propId)).append("' AND TENID='").append(STKGeneral.getPaddedTenant(tenId)).append("'  AND TENSEQNO='").append(tenSeqNo).append("'"); 
		//Logger.log(new UserRequest(),CLASS_NAME, "@@getVersionDesc-sql : "  ,sql, Logger.INFO, Logger.LOG_ABSTRACT);		 
		Vector resultVec = DbManager.getQueryResult(new Vector(), sql.toString(), poolName);
		if(resultVec != null && resultVec.size()>0)
		{
			versionVec = (Vector)resultVec.elementAt(0);
		}
		return versionVec;		
	}
	

	/**
	 * This method is get the DB date.
	 * @param poolName poolName
	 * @return DB DATE
	 * @throws Exception
	 */
	public static String getDBDate(String poolName) throws Exception
	{
		String sql 		= " SELECT TO_CHAR(SYSDATE,'MM/DD/YYYY') AS DBDATE FROM DUAL ";
		String date 	= "";		
		Logger.log( CLASS_NAME, "getDBDate() SQL : " , sql, Logger.INFO, Logger.LOG_ABSTRACT );		
		Vector results = DbManager.getQueryResult(new Vector(), sql, poolName);
		if (results != null && results.size() > 0)
			date = (String) ((Vector) results.elementAt(0)).elementAt(0);		
		Logger.log( CLASS_NAME, "getDBDate() DATE : " , date, Logger.INFO, Logger.LOG_ABSTRACT );		
		return date;	
	}
	/**
	 * This method is get the DB date.
	 * @param poolName poolName
	 * @return DB DATE
	 * @throws Exception
	 */
	public static String getDBDate(Connection dbConn) throws Exception
	{
		String sql 		= " SELECT TO_CHAR(SYSDATE,'MM/DD/YYYY') AS DBDATE FROM DUAL ";
		String date 	= "";		
		Logger.log( CLASS_NAME, "getDBDate() Connection SQL : " , sql, Logger.INFO, Logger.LOG_ABSTRACT );		
		Vector results = DbManager.getQueryResult(new Vector(), sql, dbConn);
		if (results != null && results.size() > 0)
			date = (String) ((Vector) results.elementAt(0)).elementAt(0);		
		Logger.log( CLASS_NAME, "getDBDate() Connection DATE : " , date, Logger.INFO, Logger.LOG_ABSTRACT );		
		return date;	
	}
	/**
	 * To get the SEQ_NO using SYSTEM_CONTROL table
	 * @param cntlKey1		Control key1
	 * @param aAppSession	Appsession
	 * @return int
	 * @throws Exception
	 */
	public static synchronized int getNextSequenceNo(Connection dbConnection,String cntlKey1,String sysKey) throws Exception
	{
 		long retValue = -1;		
 		
		long startTime 	= System.currentTimeMillis();

		StringBuffer sFilter = new StringBuffer();
	 	StringBuffer updateSql = new StringBuffer();
	 	StringBuffer selectSql = new StringBuffer();
	 	StringBuffer insertSql = new StringBuffer();
	 		 	
	 	int totSeq = 1;
	 	updateSql.append(" UPDATE ").append(TableConstants.TABLE_SYSTEM_CONTROL).append(" SET  SEQ_NO1 = SEQ_NO1+" ).append( totSeq ).append( " , MODIFY_TSTAMP=SYSDATE ");
		sFilter.append(" WHERE  SYS_KEY1 = '" ).append(  sysKey ).append( "' ").append( " AND CNTL_KEY1 = '" ).append( cntlKey1 ).append( "' ");
		sFilter.append( " AND CNTL_KEY2 = ' ' "	).append( " AND CNTL_KEY3 = 0 ");
		updateSql.append(sFilter.toString() );
			 
		try 
		{ 	  // Logger.log(CLASS_NAME, " getNextSequenceNo()"," updateSql.toString(): "+updateSql.toString(), Logger.INFO, Logger.LOG_ABSTRACT);
			int rowCount = GenericFactory.executeUpdateSQL(updateSql.toString(),dbConnection);
			selectSql.append("SELECT SEQ_NO1 FROM   ").append(TableConstants.TABLE_SYSTEM_CONTROL ).append( sFilter.toString());
			  // Logger.log(CLASS_NAME, " getNextSequenceNo()"," selectSql.."+selectSql.toString(), Logger.INFO, Logger.LOG_ABSTRACT);
			Vector rs = DbManager.getQueryResult(new Vector(),selectSql.toString(),dbConnection);
			
			if (rs != null && rs.size() > 0)
			{
				String tmp = (String) ((Vector)rs.get(0)).get(0);
				tmp = (tmp != null && tmp.trim().length() > 0) ? tmp.trim() : "0";
				retValue = Integer.parseInt(tmp) - totSeq;  
			}
			else
			{
				insertSql.append(" INSERT INTO  ").append(TableConstants.TABLE_SYSTEM_CONTROL).append(" (SYS_KEY1, CNTL_KEY1, CNTL_KEY2, CNTL_KEY3, SEQ_NO1, SEQ_NO2, ");
				insertSql.append( " ACTIVE_FLAG, MODIFY_USER, MODIFY_TSTAMP, CREATE_USER, CREATE_TSTAMP) ");  
				insertSql.append( " VALUES ('" ).append( sysKey ).append( "','" ).append( cntlKey1 ).append( "',' ',0," ).append( (totSeq + 1) ).append( ",0,'" ).append( Constant.CHAR_YES ).append( "',' ',SYSDATE,' ',SYSDATE)");
				  Logger.log(CLASS_NAME, " getNextSequenceNo()"," insertSql.."+insertSql.toString(), Logger.INFO, Logger.LOG_ABSTRACT);
				GenericFactory.executeUpdateSQL(insertSql.toString(), dbConnection);
			 	retValue = totSeq;
			 }
		}
		catch(Exception e)
		{
			throw e	;
		}
	   Logger.log(CLASS_NAME,"@@"," getNextSequenceNo() TOTAL TIME TAKEN : " +(System.currentTimeMillis() -startTime)+" in msecs.",Logger.DEBUG,Logger.LOG_ABSTRACT);
	   //Logger.log(CLASS_NAME, " getNextSequenceNo()"," retValue: "+ retValue, Logger.INFO, Logger.LOG_ABSTRACT);
	   return (int) retValue;
	}	
	/**
	 * to get the next SeqNo from the  table
	 * @param aAppSession	AppSession
	 * @param string tableName
	 * @param instId instance Id
	 * @param instSeqNo INST_SEQ_NO in ABWDBERR
	 * @return int SEQ_NO
	 * @throws Exception
	 */
	public static int getNextSeqNo(Connection dbConnection,String syskey1,String tableName,int instId) throws Exception
	{
		int seqNo = 1;
		String selectSql = "SELECT NVL(MAX (SEQ_NO),0)+1 FROM "+tableName+" WHERE SYS_KEY1 = '"+syskey1+"'";
		selectSql += " AND INST_ID="+instId;
		/*if(instSeqNo >0)
			selectSql += " AND INST_SEQ_NO ="+instSeqNo;*/
		//Logger.log(new UserRequest(),CLASS_NAME, "@@getNextSeqNo-selectSql : "  ,selectSql, Logger.INFO, Logger.LOG_ABSTRACT);
		Vector rs = DbManager.getQueryResult(new Vector(),selectSql.toString(),dbConnection);
		if(rs != null && rs.size()>0)
		{
			seqNo = STKGeneral.getInteger((String)((Vector)rs.elementAt(0)).elementAt(0));
		}		
		return seqNo;
	}
	/**
	 * to get the next SeqNo from the  table
	 * @param aAppSession	AppSession
	 * @param string tableName
	 * @param instId instance Id
	 * @param instSeqNo INST_SEQ_NO in ABWDBERR
	 * @return int SEQ_NO
	 * @throws Exception
	 */
	public static int getNextInstId(Connection dbConnection,String syskey1,String tableName) throws Exception
	{
		int seqNo = 1;
		String selectSql = "SELECT NVL(MAX (INST_ID),0)+1 FROM "+tableName+" WHERE SYS_KEY1 = '"+syskey1+"'";
		
		/*if(instSeqNo >0)
			selectSql += " AND INST_SEQ_NO ="+instSeqNo;*/
		//Logger.log(new UserRequest(),CLASS_NAME, "@@getNextSeqNo-selectSql : "  ,selectSql, Logger.INFO, Logger.LOG_ABSTRACT);
		Vector rs = DbManager.getQueryResult(new Vector(),selectSql.toString(),dbConnection);
		if(rs != null && rs.size()>0)
		{
			seqNo = STKGeneral.getInteger((String)((Vector)rs.elementAt(0)).elementAt(0));
		}		
		return seqNo;
	}
	/**
	 * This method is to format the GL number
	 * @param inputVal input value
	 * @return formatted GL
	 */
	public static String getFormattedGL(String inputVal)
	{
	    String returnVal = inputVal.trim();
		if (returnVal.indexOf("-") <= 0)
		{
	        returnVal = STKGeneral.getPadString(returnVal.trim(),"0",6,Constant.LEFT_PAD); 
	        returnVal = returnVal + "-0000";
		}
		else
		{
			String glMajor = returnVal.substring(0,returnVal.indexOf("-"));
			String glSub = returnVal.substring(returnVal.indexOf("-")+1,returnVal.length());
			glMajor = STKGeneral.getPadString(glMajor.trim(),"0",6,Constant.LEFT_PAD) ;
			glSub = STKGeneral.getPadString(glSub.trim(),"0",4,Constant.LEFT_PAD) ;
			returnVal = glMajor+"-"+glSub;
		}	
		 return returnVal;
	}
	/**
	 * This method is to check whether the table exists or not
	 * @param aTblName Table name
	 * @return boolean
	 * @throws Exception
	 */
	public static boolean isTblExist(String aTblName,String poolName) 
    {
        boolean retVal = true;
        StringBuffer sqlBuf = new StringBuffer();
              
        sqlBuf.append(" SELECT COUNT(*) FROM ").append(aTblName);
        try
        {
           
            Vector tempVect = DbManager.getQueryResult(new Vector(), sqlBuf.toString(), poolName);            
        }
        catch(Exception e)
        {
            retVal = false;            
        }

        return retVal;
    }
	/**
	 * This method is to check whether the table exists or not
	 * @param aTblName
	 * @param dbConnection
	 * @return
	 */
	public static boolean isTblExist(String aTblName,Connection dbConnection) 
    {
        boolean retVal = true;
        StringBuffer sqlBuf = new StringBuffer();
              
        sqlBuf.append("SELECT COUNT(*) FROM ").append(aTblName);
        try
        {           
            Vector tempVect = DbManager.getQueryResult(new Vector(), sqlBuf.toString(), dbConnection);            
        }
        catch(Exception e)
        {
            retVal = false;            
        }

        return retVal;
    }
	/**
	 * This overload function used for batch updation and throws exception
	 * 
	 * @param batchVector
	 *            Vector contains Query
	 * @param dbConnection
	 *            Connection Object
	 * @return boolean value
	 * @throws Exception
	 */
	public static boolean updateBatch(Vector batchVector, Connection dbConnection) throws Exception
	{
		boolean returnFlag = false;
		Statement stmt = null;
		if(batchVector != null && batchVector.size()>0)
		{
			try
			{
			    dbConnection.setAutoCommit(false);
				stmt = dbConnection.createStatement();
	
				stmt.clearBatch();
				for (int i = 0; i < batchVector.size(); i++)
					stmt.addBatch(batchVector.elementAt(i).toString());
	
				int[] rs = stmt.executeBatch();	
	
				stmt.close();
				stmt = null;
				returnFlag = true;
				dbConnection.commit();
			} 
			catch(Exception ex)
			{
			    Logger.log(CLASS_NAME, "Exception in updateBatch(String,String) function", ex.getMessage(), Logger.FATAL, Logger.LOG_COMMON);
			    if(stmt != null)
					stmt.close();
			    dbConnection.rollback();
			    throw ex;
			}
			finally
			{
			    if(stmt != null)
					stmt.close();
			}
		}
		return returnFlag;
	}
	
	/**
	 * This method will convert 2 digit year into 4 digit year
	 * 51-99 = 1951-1999, and 00-50 =2000-2050. 
	 * @param year2
	 * @return 4 digit year
	 * @throws Exception
	 */
	public static String  format2DigitYear(String year2) throws Exception
	{
	    String year4 = year2;
	   
	    if(year2 != null && year2.trim().length()>0 && year2.trim().length()<4)
	    {
	        int y2 = STKGeneral.getInteger(year2);	        
	        if(y2 > 50 && y2 <= 99)
	            year4 = "19"+year2.trim();
	        else if(y2 >= 00 && y2 <=50 )
	            year4 = "20"+year2.trim();
	    }	    
	    return year4;
	}
	/**
	 * This method unformates the telelphone number- removes "-","(",")" from the telephone number
	 * @param telNo Telephone Number
	 * @return String
	 */
	public static String getUnFormatedTel(String telNo)
	{
		if(telNo != "" && telNo.trim().length() > 0 )
		{
			return STKGeneral.replaceString(STKGeneral.replaceString(STKGeneral.replaceString(telNo,"-",""),")",""),"(","");
		}
		else
		{
			return "";
		
		}
	}
	public static String  getFormattedTel(String telNo)
	{		
		
		String formattedNumber = telNo;
		int formattedLength = telNo.length();
		String tempStr = "";
		String areaCode = "";
		String localNumber = "";
		if (formattedLength > 0)
		{
		    telNo = STKGeneral.getPadString(telNo,"0",10,"L");
		    formattedLength = telNo.length();
			if ( formattedLength >= 3)
			{
			    areaCode = telNo.substring(0,3);	
			    localNumber = telNo.substring(3,formattedLength);			 
			}
			if (areaCode.length() != 0 && localNumber.length() != 0 )
			{
				String tempAreaCode = STKGeneral.getPadString(areaCode,"0",3,"L");
				String tempLocalNumber = STKGeneral.getPadString(localNumber,"0",7,"L");
				formattedNumber = "(" + tempAreaCode + ")" + tempLocalNumber.substring(0,3) + "-" + tempLocalNumber.substring(3,7);
			}			
			
		}	
		return formattedNumber;
	}
	/**
	 * TO get max version from ABWPTVER table 
	 * @param sysKey1
	 * @param poolName
	 * @param propId
	 * @param tenId
	 * @param tenSeq
	 * @return
	 * @throws Exception
	 */
	public static int getMaxVersionNo(String sysKey1,String poolName,String propId,String tenId,String tenSeq) throws Exception
	{
	    int maxVersion = 0;
		Vector maxVerVec = new Vector();
		StringBuffer sql = new StringBuffer(); 
		sql.append(" SELECT MAX(TO_NUMBER(VERSIONNO)) FROM ").append(TableConstants.TABLE_ABWPTVER);		
		sql.append(" WHERE  SYS_KEY1 = '").append(sysKey1).append("'");
		sql.append(" AND  PROPID = '").append(propId).append("' AND TENID ='").append(tenId).append("' AND TENSEQNO ='").append(tenSeq).append("'");
		//Logger.log(new UserRequest(),CLASS_NAME, "@@getMaxVersionNo-sql : "  ,sql, Logger.INFO, Logger.LOG_ABSTRACT);		 
		maxVerVec = DbManager.getQueryResult(new Vector(), sql.toString(), poolName);		 
		//Logger.log(new UserRequest(),CLASS_NAME, "@@getMaxVersionNo-tenants : "  ,maxVerVec, Logger.INFO, Logger.LOG_ABSTRACT);
		if(maxVerVec != null && maxVerVec.size()>0)
		{
		    maxVersion = STKGeneral.getInteger(((String)((Vector)maxVerVec.elementAt(0)).elementAt(0)));
		}
		return maxVersion;		
	}
	
	/**
	 * This method is used to sysparam value from SYSTEM_PARAMETER
	 * @return String  
	 * @throws Exception
	 */
	public static String getSysParamValue(String sysKey1,String poolName,String sysParam)  throws Exception
	{		
		String value = "";
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT VALUE FROM ").append(TableConstants.TABLE_SYSTEM_PARAMETER);
		sql.append(" WHERE PARAM_KEY1 ='").append(sysParam).append("' AND SYS_KEY1='").append(sysKey1).append("' AND ACTIVE_FLAG='") .append(Constant.ACTIVE ) .append("'");
		//Logger.log(CLASS_NAME, " @@getSysParamValue() sql : ", sql, Logger.FATAL, Logger.LOG_ABSTRACT);
		Vector resVector = DbManager.getQueryResult(new Vector(),sql.toString(),poolName);
		if(resVector != null && resVector.size()>0)
		{
			 value = STKGeneral.nullCheck((String)((Vector)resVector.elementAt(0)).elementAt(0));			
		}
		//Logger.log(CLASS_NAME, "@@getSysParamValue() value : ",value, Logger.FATAL, Logger.LOG_ABSTRACT);
		return value;
	}
	/**
	 * This method is used to sysparam value from SYSTEM_PARAMETER
	 * @return String  
	 * @throws Exception
	 */
	public static String getSysParamValue(Connection aDbConn,String sysParam,String sysKey1)  
	{		
		String value = "";
		StringBuffer sql = new StringBuffer();
		try
		{
			sql.append("SELECT VALUE FROM ").append(TableConstants.TABLE_SYSTEM_PARAMETER);
			sql.append(" WHERE PARAM_KEY1 ='").append(sysParam).append("' AND SYS_KEY1='").append(sysKey1).append("'  AND ACTIVE_FLAG='") .append(Constant.ACTIVE ) .append("'");
			Logger.log(CLASS_NAME, " @@getSysParamValue() sql : ", sql, Logger.FATAL, Logger.LOG_ABSTRACT);
			Vector resVector = DbManager.getQueryResult(new Vector(),sql.toString(),aDbConn);
			if(resVector != null && resVector.size()>0)
			{
				 value = STKGeneral.nullCheck((String)((Vector)resVector.elementAt(0)).elementAt(0));			
			}
			//Logger.log(CLASS_NAME, "@@getSysParamValue() value : ", value, Logger.INFO, Logger.LOG_ABSTRACT);
		}catch(Exception e)
		{
		    Logger.log(CLASS_NAME, "@@getSysParamValue()  : ", e, Logger.FATAL, Logger.LOG_ABSTRACT);
		}
		return value;
	}
	private Connection dbConnection = null;
	private Statement stmt = null;
	private int sqlCount = 0;
	private int batchCount = 0;
	private int batchSize = 500; 
	private int instanceId = 0;
	private int instSeqNo = 0;
	Vector sqlVector = new Vector();
	
	public  void setBatch(Connection aDbConnection,int aBatchSize,int aInstanceId,int aInstSeqNo) throws Exception
	{	    
	    dbConnection = aDbConnection;
	    dbConnection.setAutoCommit(false);
		stmt = dbConnection.createStatement();
		stmt.clearBatch();
		batchSize = aBatchSize;
		sqlVector = new Vector();
		instanceId = aInstanceId;
		instSeqNo = aInstSeqNo;
	}
	
	/**
	 * This overload function used for batch updation and throws exception
	 * 
	 * @param batchVector
	 *            Vector contains Query
	 * @param dbConnection
	 *            Connection Object
	 * @param batchSize
	 * 			batch size
	 * @return boolean value 
	 * @throws Exception
	 */
	
	public  boolean updateBatch(String tblName ,String sql,boolean isFinal) throws Exception
	{
		boolean returnFlag = false;
		try
		{	
		    if((sql != null && sql.trim().length() > 0))
		    {
		        sqlVector.add(sql);
		        stmt.addBatch(sql);
				sqlCount++;	
		    }
		    if(isFinal || (sql != null && sql.trim().length() > 0) )
		    {		        
				if( (sqlCount >= batchSize) || (isFinal && sqlCount > 0 ) )
				{
				    batchCount++;	
				    //Logger.log(CLASS_NAME, "updateBatch  sqlVector", sqlVector, Logger.FATAL, Logger.LOG_ABSTRACT);
					   
				    int[] rs = stmt.executeBatch();
				    
				    Logger.log(CLASS_NAME, "Executing Batch ", "@@batch #"+batchCount+" #tablename .."+tblName, Logger.FATAL, Logger.LOG_ABSTRACT);
				    stmt.clearBatch();
				    sqlVector = new Vector();
				    sqlCount = 0;		
				    
				    
				}	
		    }
		    if(isFinal)
		    {
		        batchCount =0;
		        dbConnection.commit();
		        Logger.log(CLASS_NAME, "Executing Batch ", "@@RESET BATCHCOUNT", Logger.FATAL, Logger.LOG_ABSTRACT);
				  
		    }
		    
		}
		catch(Exception ex)
		{ 
		    Logger.log(CLASS_NAME, "Exception in updateBatch(String,String,int) function", ex.getMessage(), Logger.FATAL, Logger.LOG_ABSTRACT);
		   // Logger.log(CLASS_NAME, "INSIDE catch "," syskey1.."+syskey1+"#poolName.."+poolName, Logger.FATAL, Logger.LOG_ABSTRACT);
		    if(sqlVector != null && sqlVector.size() >0 )
		        writeAbsMakeDBLog(sqlVector,syskey1,poolName,instanceId,instSeqNo);
		    
		    if(stmt != null)
				stmt.close();
		    dbConnection.rollback();
		    throw ex;
		}
		finally
		{
		   
		    if(isFinal)
		    {
		        
		        if(stmt != null)
		            stmt.close();
		    }
		}
		return returnFlag;
	}
	
	
	public void setErrLogPath(String aPath)
	{
	    errLogPath = aPath;
	}
	/**
     * This method will write  the SQL  vector in a file when a exception occurs
     * @param errLogVec
     * @param sysKey1
     * @param poolName
     * @throws Exception
     */
    public  void writeAbsMakeDBLog(Vector errLogVec,String sysKey1,String poolName,int instanceId,int instSeqNo) throws Exception
    {
        try
        {            
            if(errLogPath == null || errLogPath.trim().length()==0)
            {
                errLogPath = getSysParamValue(sysKey1,poolName,Constant.SYS_PARAM_ABS_ERRLOG_PATH);
            }
            String fileName = errLogPath +"\\AbsMakeDBLog_"+sysKey1.trim()+"_"+instanceId+"_"+instSeqNo+".txt";
            Logger.log(new UserRequest(),CLASS_NAME, "@@writeAbsMakeDBLog filePath : ", errLogPath, Logger.FATAL, Logger.LOG_ABSTRACT);
	        FileOutputStream fw = new FileOutputStream(fileName,true);      	      
	        fw.write(("\n"+errLogVec.toString()).getBytes());      	        
	        fw.close();	
	        Logger.log(new UserRequest(),CLASS_NAME, "@@writeAbsMakeDBLog  : ", "ERROR LOG WRITTEN.."+fileName, Logger.FATAL, Logger.LOG_ABSTRACT);
		       
        }catch(Exception e)
        {
            e.printStackTrace();
            throw e;
        }
    }
    /**
     * This method will write  the SQL  vector in a file when a exception occurs
     * @param errLogVec
     * @param sysKey1
     * @param poolName
     * @throws Exception
     */
    public  void writeAbsLog(Connection dbConn,Vector errLogVec,String sysKey1,int instanceId,int instSeqNo) throws Exception
    {
        try
        {            
            if(errLogPath == null || errLogPath.trim().length()==0)
            {
                errLogPath = getSysParamValue(dbConn,Constant.SYS_PARAM_ABS_ERRLOG_PATH,sysKey1);
            }
            String fileName = errLogPath +"\\AbsMakeDBLog_"+instanceId+"_"+instSeqNo+".txt";
            Logger.log(new UserRequest(),CLASS_NAME, "@@writeAbsMakeDBLog filePath : ", errLogPath, Logger.FATAL, Logger.LOG_ABSTRACT);
	        FileOutputStream fw = new FileOutputStream(fileName,true);      	      
	        fw.write(("\n"+errLogVec.toString()).getBytes());      	        
	        fw.close();	
	        Logger.log(new UserRequest(),CLASS_NAME, "@@writeAbsMakeDBLog  : ", "ERROR LOG WRITTEN.."+fileName, Logger.FATAL, Logger.LOG_ABSTRACT);
		       
        }catch(Exception e)
        {
            e.printStackTrace();
            throw e;
        }
    }
    


	/**
	 * This method is insert the status of makeDB/rollback into  ABWINST table
	 * 
	 * @param instId instance Id (INST_ID in DataDT table)
	 * @param funcType function type (MAKEDB /ROLLBACK)
	 * @param status  Status of the process (Success/Failed)
	 * @param comments Comments about the process
	 * @throws Exception
	 */
	/**
	 * This method is insert the status of makeDB/rollback into  ABWINST table
	 * @param aDbConnection
	 * @param instId  instance Id (INST_ID in DataDT table)
	 * @param funcType  function type (MAKEDB /ROLLBACK)
	 * @param status  Status of the process (Success/Failed)
	 * @param comments Comments about the process
	 * @param instSeqNo seqno in INST table
	 * @param syskey 
	 * @param userId
	 * @return
	 * @throws Exception
	 */
	public static String insertABWINST(Connection dbConn,int instId ,String funcType,
	        String status,String comments,int instSeqNo,String syskey,String userId)  throws Exception
	{
		
		StringBuffer sql = new StringBuffer();

		sql.append("INSERT INTO ").append(TableConstants.TABLE_ABWINST);
		sql.append("(SYS_KEY1, SEQ_NO, INST_ID, FUNCTYPE, STATUS, COMMENTS,  CREATE_USER, CREATE_TSTAMP)");
		sql.append(" VALUES ('").append(syskey).append("',").append(instSeqNo).append(",").append(instId).append(",'").append(funcType).append("','");
		sql.append(status).append("','").append(STKGeneral.blankQuoteCheck(comments)).append("','");		
		sql.append(userId + "'," + Constant.CURRENT_TIME_STAMP + ")");
		
		Logger.log(CLASS_NAME, " insertABWINST() sql : ", sql.toString(), Logger.FATAL, Logger.LOG_ABSTRACT);
    	GenericFactory.executeUpdateSQL(sql.toString(),dbConn);
		return status;
	}
	/**
     * This function is used to get the Abstract Archive 
     * error details from ABWDBERR and ABWINST tables.
     * @param instanceId Instace Id
     * @param seqNumber seqNumber
     * @return  Vector
     * @throws Exception
     */
    
    public Vector getArchiveData(Connection dbConn,int instanceId,int seqNumber,String sysKey1) throws Exception
    {
        Vector dataVec = new Vector();
       // String sysKey1 = appSession.getSysKey1();
        StringBuffer sql = new StringBuffer(); 
      
     
        sql.append("SELECT  TABLENAME, ERRDESC, ERRDETAIL FROM ").append(TableConstants.TABLE_ABWDBERR);     
        sql.append("  WHERE SYS_KEY1 = '").append(sysKey1).append("'");
        sql.append("   AND INST_SEQ_NO =").append(seqNumber).append("  AND INST_ID = ").append(instanceId);       
       // Logger.log(new UserRequest(),CLASS_NAME, "@@getArchiveData sql Query : ", sql.toString(), Logger.INFO, Logger.LOG_ABSTRACT);
           
        dataVec = DbManager.getQueryResult(new Vector(), sql.toString(),  dbConn);
        
        return dataVec;
    }
    public String updateABWINST(Connection dbConn,int instId ,String funcType,String status,
	        String comments,int iInstSeqNO,String sysKey1)  throws Exception
	{
	    if(status.trim().equalsIgnoreCase(Constant.STATUS_SUCCESS))
		{
		
			Vector dataVector = getArchiveData(dbConn,instId,iInstSeqNO,sysKey1);
			//Logger.log(CLASS_NAME, " updateABWINST() dataVector: ", dataVector, Logger.INFO, Logger.LOG_ABSTRACT);
			if(dataVector!= null && dataVector.size() >0)	
			{
			    //Logger.log(CLASS_NAME, " updateABWINST() funcType: ", funcType, Logger.INFO, Logger.LOG_ABSTRACT);
				if(funcType.trim().equalsIgnoreCase(Constant.ABS_FUNC_TYPE_MAKEDB))
				{
					status = Constant.STATUS_SUCCESS_WARN;
					comments = "Success with Warning";
				}
				else
				{
					status = "RW";
					comments = "Rollback with Warning";
				}
			}
		}
	    StringBuffer sql = new StringBuffer(); 
		sql.append(" UPDATE ").append(TableConstants.TABLE_ABWINST)
			.append(" SET  STATUS = '").append(status).append("',")
			.append(" COMMENTS ='").append(comments).append("'")			
			.append( " WHERE SYS_KEY1='").append(sysKey1).append("'")
			.append( " AND INST_ID = ").append(instId)
			.append( " AND SEQ_NO = ").append(iInstSeqNO);
		
		Logger.log(CLASS_NAME, " updateABWINST() sql : ", sql, Logger.INFO, Logger.LOG_ABSTRACT);
		GenericFactory.executeUpdateSQL(sql.toString(),dbConn);
		
		return status;
	}
	
    /**
     * This method is to the DB connection for target DB by getting the DB details of the target syskey
     * @throws Exception
     */
    public static Vector getTargetDBConnection(String targetSyskey,String commPoolName) throws Exception
    {     
        Vector retVect = new Vector();
        Connection targetDBConn = null;
        String user = " ";
	    String password = " ";
	    String dirverParam	= "";
	    String targerdbName = "";
	    try
	    {
		    Vector targetDBDtl = getDBDetails(targetSyskey,commPoolName);
		    Logger.log( CLASS_NAME, "@@getTargetDBConnection()", targetDBDtl, Logger.FATAL, Logger.LOG_ABSTRACT);
		    if(targetDBDtl != null && targetDBDtl.size()>0)
		    {
		        Vector innerVec = (Vector)targetDBDtl.elementAt(0);
		        user = STKGeneral.nullCheck((String)innerVec.elementAt(0)).trim();
			    password = STKGeneral.nullCheck((String)innerVec.elementAt(1)).trim();
			    dirverParam	= STKGeneral.nullCheck((String)innerVec.elementAt(2)).trim();
			    targerdbName = dirverParam.substring((dirverParam.lastIndexOf(":")+1), dirverParam.length()); 
			    retVect.add(user);
			    targetDBConn = getDBConnection(user,password,dirverParam);
			    retVect.add(targetDBConn);
			    retVect.add(targerdbName);
		    }
		    
	    }
        catch (Exception e)
        {
            /*if(sourceOfError.trim().length() == 0)
                sourceOfError = "Error while getting target DB connection.";*/
            throw e;
        }
        finally
        {
           return  retVect;
        }
    }
    /**
     * This method is to get the DB details from DB_INSTACNE table (epmcomm schema)
     * @return  DB detils
     * @throws Exception
     */
    public static Vector getDBDetails(String aSysKey,String commPoolName) throws Exception
    {
        Vector dbDetails = null;
        StringBuffer sql = new StringBuffer();
        String custId = "";
        String sysId = "";
        if(aSysKey != null && aSysKey.length()>0)
        {
            custId = aSysKey.substring(0,aSysKey.length()-2);
            sysId = aSysKey.substring(aSysKey.length()-2,aSysKey.length());
        }
        Logger.log( CLASS_NAME, "@@getDBDetails()",  "@@custId.."+custId+"##sysId.."+sysId, Logger.FATAL, Logger.LOG_ABSTRACT);
        sql.append("SELECT db_user, db_password, driver_parameters FROM ");
        sql.append(TableConstants.M_DB_INSTANCE_TABLE ).append(" dbi, ").append(TableConstants.M_SYSTEM_MAST_TABLE).append(" s ");
        sql.append(" WHERE dbi.db_inst_id = s.db_inst_id AND s.cust_id = '").append(custId).append("' AND s.system_id = '").append(sysId).append("'");
        Logger.log( CLASS_NAME, "@@getDBDetails()",  sql.toString(), Logger.FATAL, Logger.LOG_ABSTRACT);
        dbDetails = DbManager.getQueryResult(new Vector(), sql.toString(), commPoolName);
        return dbDetails;
    }
     
    /**
     * To get target DB connection
     * @param user
     * @param pwd
     * @param url
     * @throws Exception
     */
    private static boolean usable 	= false;
	private static String hostName 	= "";
	private static Driver myDriver 	= null;
    private static Connection getDBConnection(String user,String pwd, String url) throws Exception
    {
       
            if (usable == false)
    	    {
    		    myDriver = (java.sql.Driver)Class.forName(ORACLE_DRIVER).newInstance();
    		    DriverManager.registerDriver(myDriver);
    		    usable = true;
    	    }
    	
    	  /*  String user = "EPMSYNCCOPY";
    	    String pwd 	= "EPMSYNCCOPY";
    	    String url	= "jdbc:oracle:thin:@192.1.2.228:1521:elite254";*/
    	    
    	  	Properties props = new Properties();
    	  	props.setProperty(USER, user);
    	  	props.setProperty(PASSWORD, pwd);
    	  	return myDriver.connect(url, props);
    	  	//Logger.log( CLASS_NAME, "@@getTargetDBDetails()",  "@@ctargetDBConn.."+targetDBConn, Logger.FATAL, Logger.LOG_ABSTRACT);
    	      
    }
    /**
	 * This method is to store  the errors occured while making DB in the table ABWDBERR(Without using session)
	 
	 * @param tableName		Table name
	 * @param exceptionDesc	Exception Description
	 * @param exceptionTrace Exception trace
	 * @throws Exception
	 */
	public void logError(Connection dbConn,String userId,String sysKey,int instanceId, int instSeqNo,String tableName,String exceptionDesc, String exceptionTrace)  throws Exception
	{
		exceptionDesc = (exceptionDesc.length() > 100 ? exceptionDesc.substring(0,99) : exceptionDesc);
		exceptionTrace = (exceptionTrace.length() > 500 ? exceptionTrace.substring(0,499) : exceptionTrace);
		int  seqNo = getNextSeqNo(dbConn,sysKey,TableConstants.TABLE_ABWDBERR,instanceId);
		StringBuffer sbSql = new StringBuffer();
		sbSql.append("INSERT INTO ABWDBERR ");
		sbSql.append(" (SYS_KEY1, INST_ID,INST_SEQ_NO,SEQ_NO,TABLENAME, ERRDESC, ERRDETAIL, MODIFY_USER, MODIFY_TSTAMP, CREATE_USER, CREATE_TSTAMP) "); 
		sbSql.append(" VALUES ('" );
		sbSql.append(	sysKey + "',");
		sbSql.append(	instanceId + "," );
		sbSql.append(	instSeqNo + "," );
		sbSql.append(	seqNo + ",'" );
		sbSql.append(	tableName + "','" );
		sbSql.append(STKGeneral.blankQuoteCheck(exceptionDesc) + "', '");
		sbSql.append(STKGeneral.blankQuoteCheck(exceptionTrace) + "','");
		sbSql.append(userId + "'," + Constant.CURRENT_TIME_STAMP + ",'"); 
		sbSql.append(userId + "'," + Constant.CURRENT_TIME_STAMP + ")"); 
		//Logger.log(new UserRequest(),CLASS_NAME, "@@logError-sbSql : "  ,sbSql, Logger.INFO, Logger.LOG_ABSTRACT);
		GenericFactory.executeUpdateSQL(sbSql.toString(),dbConn);	      
	}
    /**
     * This method will return the collection of abstype and its class
     * @param sysKey1
     * @param poolName
     * @return
     * @throws Exception
     */
    public static Hashtable getAbsClassHash(String sysKey1,String poolName) throws Exception
	{
		Hashtable absClassHash = new Hashtable();
		/*StringBuffer sql = new StringBuffer(); 
		sql.append(" SELECT ABSTYPE,ABS_CLASS FROM ").append( TableConstants.TABLE_ABWABTP);
		sql.append(" WHERE SYS_KEY1 = '").append(sysKey1).append("'"); 
		Logger.log(new UserRequest(),CLASS_NAME, "@@getAbsClassHash-sql : "  ,sql, Logger.FATAL, Logger.LOG_ABSTRACT);		 
		absClassHash = DbManager.getQueryResultHash(new Vector(), sql.toString(),"ABSTYPE", poolName);		 
		Logger.log(new UserRequest(),CLASS_NAME, "@@getAbsClassHash-absClassHash : "  ,absClassHash, Logger.INFO, Logger.LOG_ABSTRACT);*/
		absClassHash = getAbsClassHash(sysKey1,poolName,TableConstants.TABLE_ABWABTP);
		return absClassHash;		
	}
    /**
     * 
     * This method will return the collection of abstype and its class
     * @param sysKey1
     * @param poolName
     * @return
     * @throws Exception
     */
    public static Hashtable getAbsClassHash(String sysKey1,String poolName,String tpTblName) throws Exception
	{
        
		Hashtable absClassHash = new Hashtable();
		StringBuffer sql = new StringBuffer(); 
		sql.append(" SELECT ABSTYPE,ABS_CLASS FROM ").append( tpTblName);
		sql.append(" WHERE SYS_KEY1 = '").append(sysKey1).append("'"); 
		Logger.log(new UserRequest(),CLASS_NAME, "@@#getAbsClassHash-sql : "  ,sql, Logger.FATAL, Logger.LOG_ABSTRACT);		 
		absClassHash = DbManager.getQueryResultHash(new Vector(), sql.toString(),"ABSTYPE", poolName);		 
		Logger.log(new UserRequest(),CLASS_NAME, "@@getAbsClassHash-absClassHash : "  ,absClassHash, Logger.INFO, Logger.LOG_ABSTRACT);		
		return absClassHash;		
	}
    /**
     * This method will return the collection of abstype and its class
     * @param dbConn
     * @return
     * @throws Exception
     */
    public static Hashtable getAbsClassHash(Connection dbConn,String sysKey1) throws Exception
	{
		Hashtable absClassHash = new Hashtable();
		StringBuffer sql = new StringBuffer(); 
		sql.append(" SELECT ABSTYPE,ABS_CLASS FROM ").append( TableConstants.TABLE_ABWABTP);
		sql.append(" WHERE SYS_KEY1 = '").append(sysKey1).append("'"); 
		Logger.log(new UserRequest(),CLASS_NAME, "@@getAbsClassHash-sql : "  ,sql, Logger.FATAL, Logger.LOG_ABSTRACT);		 
		absClassHash = DbManager.getQueryResultHash(new Vector(), sql.toString(),"ABSTYPE", dbConn);		 
		Logger.log(new UserRequest(),CLASS_NAME, "@@getAbsClassHash-absClassHash : "  ,absClassHash, Logger.INFO, Logger.LOG_ABSTRACT);		
		return absClassHash;		
	}
	/**
	 * This method will return the absclass of the particular abstype
	 * @param absClassHash
	 * @param absType
	 * @return
	 * @throws Exception
	 */
	public static String getAbsClass(Hashtable absClassHash,String absType) throws Exception
	{
		
		String absClass = "";
		if(absClassHash != null && absClassHash.size()>0)
		{
			if(absClassHash.get(absType.trim()) != null)
			{
				Vector inVec = (Vector)absClassHash.get(absType);
				absClass = STKGeneral.nullCheck(((String)inVec.elementAt(1)).trim());								
			}
		}
		
		return absClass;
	}
	/**
	 * To get the max seq from ABWDELLOG
	 * @param sysKey1
	 * @param poolName
	 * @return
	 * @throws Exception
	 */
	private static int getMaxSeqFromLog(String sysKey1,String poolName) throws Exception
	{
	    int maxSeq = 0;
	    String sql ="SELECT MAX(SEQ_NO) FROM ABWDELLOG WHERE SYS_KEY1 ='"+sysKey1.trim()+"'";
	    Logger.log(new UserRequest(),CLASS_NAME, "@@getAbsClassHash-sql : "  ,sql, Logger.FATAL, Logger.LOG_ABSTRACT);		 
		Vector resVec = DbManager.getQueryResult(new Vector(), sql, poolName);
		if(resVec != null && resVec.size()>0)
		{
		    String max = (String)((Vector)resVec.elementAt(0)).elementAt(0);
		    maxSeq = STKGeneral.getInteger(STKGeneral.nullCheck(max));
		}
		return maxSeq;
	}
	/**
	 * This method is to log the details in ABWDELLOG table when a version is deleted from data tables
	 * @param appSession
	 * @param details
	 * @throws Exception
	 */
	public static void logDelete(AppSession appSession,Vector details) throws Exception
	{
	      
	    StringBuffer insertSql = new StringBuffer();
	    Vector batchVec = new Vector();
	    
	    String userId = appSession.getUserID();
	    String sysKey1 = appSession.getSysKey1();
	    String poolName = appSession.getJDBCPoolName();
	    int seqNo = getMaxSeqFromLog(sysKey1,poolName) ;
	    
	    if(details != null && details.size()>0)
	    {
	        String absType = "";
	        String propId = "";
	        String tenId = "";
	        String tenSeqNo = "";
	        String  version = "";
	        String desc  = "";
	            
	        for(int i=0;i<details.size();i++)
	        {
	            Vector tempVec = (Vector)details.elementAt(i);
	            absType = (String)tempVec.elementAt(0);
		        propId = (String)tempVec.elementAt(1);
		        tenId = (String)tempVec.elementAt(2);
		        tenSeqNo = (String)tempVec.elementAt(3);
		        version = (String)tempVec.elementAt(4);
		        desc  = (String)tempVec.elementAt(5);
		        seqNo++;
		        insertSql = new StringBuffer();
			    insertSql.append(" INSERT INTO  ").append("ABWDELLOG")
			    			.append(" (SYS_KEY1, SEQ_NO, ABSTYPE, PROPID, TENID, TENSEQNO, VERSIONNO, DESCRIPTION, CREATE_USER, CREATE_TSTAMP )");	  
			    insertSql.append( " VALUES ('" ).append( sysKey1 ).append( "'," ).append( seqNo ).append( ",'" )
			    		.append( absType).append( "','" ).append(STKGeneral.getPaddedProperty(propId) ).append( "','")
			    		.append( STKGeneral.getPaddedTenant(tenId)).append( "','" ).append( STKGeneral.getPadString(tenSeqNo,"0", 2, Constant.LEFT_PAD)).append( "','")
			    		.append( version).append( "','" ).append(STKGeneral.blankQuoteCheck(desc) ).append( "','").append(userId ).append( "',SYSDATE)");
			    
			    batchVec.add(insertSql.toString());
	        }
		    Logger.log(CLASS_NAME, " logDelete()batchVec..",batchVec, Logger.INFO, Logger.LOG_ABSTRACT);
		  
		    GenericFactory.updateBatch(batchVec, poolName);
	    }
	}
}
