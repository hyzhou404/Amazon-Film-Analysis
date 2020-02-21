package Query;

import Global.Results;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class neo4jQuery implements abstractQuery {

    private Result exec(String query){
        long tic = System.currentTimeMillis();
        Result executeResult = db.execute(query);
        long toc = System.currentTimeMillis();
        Results.Neo4jTime = (int)(toc-tic);
        return executeResult;
    }

    private String getResStr(Result executeResult, String key){
        StringBuilder res = new StringBuilder();
        HashSet<String> rows = new HashSet<>();
        while(executeResult.hasNext()){
            Object row = executeResult.next().get(key);
            if(row != null && !rows.contains(row.toString().trim())) {
                rows.add(row.toString().trim());
                res.append(row.toString().trim());
                res.append("\n");
            }
        }
        if (res.toString().equals("")){
            res.append("未查询到结果");
        }
        return res.toString();
    }



    /* *************************************************************************************/
    /*--------------------------------------------------------------------------------------
         所有接口需要使用System.currentTimeMillis()记录运行时间，然后修改Results里相应的时间
     -------------------------------------------------------------------------------------*/
    /* ************************************************************************************/

    private static GraphDatabaseService db;
    private static Boolean hasCreated = false;

    neo4jQuery(){
        if(!hasCreated) {
            GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
            db = dbFactory.newEmbeddedDatabaseBuilder(new File("src/main/resources/data/movie.db")).newGraphDatabase();
            hasCreated = true;
        }
    }

    @Override
    public String showAllMovies(String arg) {
        String query = "MATCH (m:MOVIE) RETURN m.name LIMIT 1000";
        return getResStr(exec(query), "m.name");
    }

    @Override
    public String showAllDirectors(String arg) {
        String query = "MATCH (d:DIRECTOR) RETURN d.name LIMIT 1000";
        return getResStr(exec(query), "d.name");
    }

    @Override
    public String showAllActors(String arg) {
        String query = "MATCH (actor:ACTOR) RETURN actor.name LIMIT 1000";
        return getResStr(exec(query), "actor.name");
    }

    @Override
    public String showAllMovieTypes(String arg) {
        String query = "MATCH (g:GENRE) RETURN g.genreName LIMIT 1000";
        return getResStr(exec(query), "g.genreName");
    }

    @Override
    public String ymMovieNum(String arg) {
        //查询&计时
        String[] tmp = arg.split("#");
        String time = tmp[0].trim()+"/"+tmp[1].trim();
        String query = "MATCH (m:MOVIE) WHERE m.release_time =~ '"+time+".*' RETURN count(*)";
        return String.valueOf(exec(query).next().get("count(*)"));
    }

    @Override
    public String movieEditionNum(String arg) {
        //查询
        String query = "MATCH (:MOVIE{name: '"+arg+"'})-[HAS_EDITION]->(format:FORMAT)"+
                        " RETURN format.formatName";
        Result executeResult = exec(query);
        //生成结果
        StringBuilder res = new StringBuilder();
        HashSet<String> formats = new HashSet<>();
        while(executeResult.hasNext()){
            Object Format = executeResult.next().get("format.formatName");
            if(Format != null && !formats.contains(Format.toString())){
                formats.add(Format.toString());
                res.append(Format.toString());
                res.append("\n");
            }
        }
        return "共有"+formats.size()+"个版本\n"+res.toString();
    }

    @Override
    public String movieByDirectorNum(String arg) {
        String query = "MATCH (:DIRECTOR {name: '"+arg+"'})-[DIRECT]->(m:MOVIE) RETURN count(*)";
        return String.valueOf(exec(query).next().get("count(*)"));
    }

    @Override
    public String movieMainByActorNum(String arg) {
        String query = "MATCH (:ACTOR {name: '"+arg+"'})-[rel:ACT]->(m:MOVIE) " +
                        "WhERE rel.Star = 'TRUE' RETURN count(*)";
        return String.valueOf(exec(query).next().get("count(*)"));
    }

    @Override
    public String movieByActorNum(String arg) {
        String query = "MATCH (:ACTOR {name: '"+arg+"'})-[ACT]->(m:MOVIE) RETURN count(*)";
        return String.valueOf(exec(query).next().get("count(*)"));
    }

    @Override
    public String typeNum(String arg) {
        String query = "MATCH (:GENRE {genreName: '"+arg+"'})<-[HAS_EDITION]-(m:MOVIE)"+
                " RETURN count(*)";
        return String.valueOf(exec(query).next().get("count(*)"));
    }

    @Override
    public String frequentActors(String arg) {
        String query = "MATCH (:ACTOR {name: '"+arg+"'})-[rel1:ACT]->()<-[rel2:ACT]-(a:ACTOR)\n" +
                "WITH count(a.name) AS num, a.name AS name\n" +
                "WHERE num >= 3\n" +
                "RETURN name";
        return getResStr(exec(query), "name");
    }

    @Override
    public String frequentDirectors(String arg) {
        String query = "MATCH (a:ACTOR {name: '"+arg+"'})-[:ACT]->()<-[:DIRECT]-(d:DIRECTOR)\n" +
                "WITH count(d.name) AS num, d.name AS name\n" +
                "WHERE num >= 3\n" +
                "RETURN name";
        return getResStr(exec(query), "name");
    }

    @Override
    public String typeBestTime(String arg) {
        String query = "MATCH (g:GENRE {genreName: '"+arg+"'})<-[HAS_EDITION]-(m:MOVIE)\n" +
                        "WITH toInteger(m.review_num) AS num, m.release_time AS time\n" +
                        "WHERE time <> 'null'\n"+
                        "RETURN num, time ORDER BY num DESC LIMIT 3";
        return getResStr(exec(query), "time");
    }

    @Override
    public String mostDayForType(String arg) {
        String query = "MATCH (:GENRE {genreName: '"+arg+"'})<-[:HAS_GENRE]-(m:MOVIE)\n" +
                "WITH count(*) AS num, m.release_time AS time\n" +
                "RETURN num, time ORDER BY num DESC";
        Result execRes = exec(query);
        String maxTime = "";
        while(execRes.hasNext()) {
            Object row = execRes.next().get("time");
            if(row != null){
                maxTime = row.toString();
                break;
            }
        }
        if(maxTime.equals("")){
            maxTime = "电影类型输入错误";
        }
        return maxTime;
    }

    @Override
    public String languageByActor(String arg) {
        String[] tmp = arg.split("#");
        String query = "MATCH (a:ACTOR {name: '"+tmp[0]+"'})-[:ACT]->(m:MOVIE)-[:HAS_LANGUAGE]->(l:LANGUAGE)\n" +
                "WHERE l.languageName = '"+tmp[1]+"'\n" +
                "RETURN m.name";
        return getResStr(exec(query), "m.name");
    }

    @Override
    public String moreThan(String arg) {
        String[] tmp = arg.split("#");
        String query1 = "MATCH (m:MOVIE)\n" +
                "WHERE toInteger(m.avg_score) > 2 AND m.release_time =~ \""+tmp[0]+".*\"\n" +
                "RETURN count(*)";
        String query2 = "MATCH (m:MOVIE)\n" +
                "WHERE toInteger(m.avg_score) > 2 AND m.release_time =~ \""+tmp[1]+".*\"\n" +
                "RETURN count(*)";
        long tic = System.currentTimeMillis();
        int difference = Integer.parseInt(db.execute(query1).next().get("count(*)").toString()) - Integer.parseInt(db.execute(query2).next().get("count(*)").toString());
        long toc = System.currentTimeMillis();
        Results.Neo4jTime = (int)(toc-tic);
        return String.valueOf(difference);
    }
}
