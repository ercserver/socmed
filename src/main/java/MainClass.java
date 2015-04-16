import api.IDbHandler;
import db.DbHandler;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ohad on 31/3/2015.
 */
public class MainClass {
    public static void main(String args[]){
        /*I*/DbHandler dbHandler = new DbHandler(false);
        //dbHandler.updateTable("enum", new JSONObject().put("tableName","'table'").put("columnName","'col'"), "enumValue", "'no'");

        System.out.println(dbHandler.isCommunityMemberExists(99));



    }
}
