package api;

import org.json.JSONArray;
import org.json.JSONObject;

public interface IDbHandler {
    /*public DbHandler(boolean createTables); //The constructor. Use 'false' to onl establish a connection*/
    public boolean isCommunityMemberExists(int cmid); // Returns 'true' if exists in the db

    public JSONObject getEnums(JSONObject jo); // Returns the description of a code, or the other way around.
    /* {"TableName":"table1","ColumnName":"gender","EnumCode":2}*/
    /* {"TableName":"table1","ColumnName":"gender","EnumValue":"male"}*/
    public JSONArray getRegistrationFields(int userType); // userType code defined by an enum

    public JSONObject getUserByParameter(JSONObject parms); // Returns a user by a parameter (mail, cmid).
    /* e.g. {'email':'bla@gmail.com'} */

    public void updateUserDetails(JSONObject details); // Use the field 'CMID' to update desired columns

    public JSONObject getFrequency(JSONObject freqParams);

    public JSONObject getDefaultInEmergency(String state);

    public JSONObject getRejectCodes(); // Returns all the available patient's reject codes

    public JSONObject getWaitingPatientsCMID(int status, int docCMID); // Returns a json containing all the relevant cmids

    public void updateUrgentInRefreshDetailsTime(int CMID, String fieldName, int urgentBit);

    public int addNewCommunityMember(JSONObject details); // Should be used with the appropriate registration fields. Returns the new CMID

    public JSONObject getAuthenticationMethod(String state); // Returns a json of all available methods of authentication

    public JSONObject getEmailOfDoctorsAuthorizer(String state);

    public JSONObject getLoginDetails(String email);

    public void updateLastRefreshTime(JSONObject params); // Should be given cmid and field name

    public JSONArray getAllRefreshTimes(); // Returns all the fields of all the members with last refresh times

    

}
