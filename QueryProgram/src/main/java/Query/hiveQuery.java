package Query;

import Global.Results;
import basic.hiveConn;

public class hiveQuery implements abstractQuery {
    /* *************************************************************************************/
    /*--------------------------------------------------------------------------------------
         所有接口需要使用System.currentTimeMillis()记录运行时间，然后修改Results里相应的时间
     -------------------------------------------------------------------------------------*/
    /* ************************************************************************************/
    private hiveConn hive = new hiveConn();
    private String sql = "";
    @Override
    public String showAllMovies(String arg) {
        sql = "select * from movies";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String showAllDirectors(String arg) {
        sql = "select * from directors";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String showAllActors(String arg) {
        sql = "select * from actors";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String showAllMovieTypes(String arg) {
        sql = "select * from movieTypes";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String ymMovieNum(String arg) {
        arg = arg.replace('#','-');
        sql = "select count(*) from movies where release_time like'%"+arg+"%'";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String movieEditionNum(String arg) {
        sql = "select m.name,count(*) from movies as m,hasEdition as h where m.id = h.movieId and m.name = '"+arg+"' group by m.name";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String movieByDirectorNum(String arg) {
        sql = "select d.name,count(*) from directors as d,direct as dd where d.id = dd.directorId and d.name = '"+arg+"' group by d.name";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String movieMainByActorNum(String arg) {
        sql = "select a.name,count(*) from actors as a,act as aa where a.id = aa.actorId and a.name = '"+arg+"' and aa.isStar = 'TRUE' group by a.name";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String movieByActorNum(String arg) {
        sql = "select a.name,count(*) from actors as a,act as aa where a.id = aa.actorId and a.name = '"+arg+"' group by a.name";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String typeNum(String arg) {
        sql = "select count(*) from hasGenre where genreName = '"+arg+"'";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String frequentActors(String arg) {
        sql = "select name from actors where id in " +
                "(SELECT actorId FROM  (select actorId,movieId from act where movieId in " +
                "(select movieId from act where actorId in (select id from actors where name='"+arg+"'))) " +
                "as temp group by actorId having count(movieId)>2) ";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String frequentDirectors(String arg) {
        sql = "select name from directors where id in " +
                "(SELECT directorId FROM  (select directorId,movieId from direct where movieId in " +
                "(select movieId from act where actorId in (select id from actors where name='"+arg+"'))) " +
                "as temp group by directorId having count(movieId)>2) ";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String typeBestTime(String arg) {
        sql = "select month, max(sum_num) from " +
                "(select MONTH(release_time) as month,sum(review_num) as sum_num from movies where id in " +
                "(select movieId from hasGenre where genreName ='"+arg+"') " +
                "group by MONTH(release_time)) as temp " +
                "group by month";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String mostDayForType(String arg) {
        sql = "select release_time from(select count(id) as idcount,release_time from" +
                "(select id,release_time from movies where release_time is not null and id in " +
                "(select movieId from hasGenre where genreName='"+arg+"'))as table1 group by release_time)as table2 " +
                "where idcount=" +
                "(select max(idcount) from (select count(id) as idcount,release_time from " +
                "(select id,release_time from movies where release_time is not null and id in " +
                "(select movieId from hasGenre where genreName='"+arg+"'))as table3 group by release_time)as table4)";
        Results.HiveTime = hive.getQuery(sql);
        return "";
    }

    @Override
    public String languageByActor(String arg) {
        String actor = new String();
        String language = new String();
        char temp[] = arg.toCharArray();
        int i;
        for(i=0;temp[i]!='#';i++)
        {
            actor=actor+temp[i];
        }
        i=i+1;
        for(;i<temp.length;i++)
        {
            language=language+temp[i];
        }
        sql = "select name from movies where id in " +
                "(select movie_id from hasLanguage where language_name='"+language+"' " +
                "and movie_id in (select movieId from act where actorId in" +
                "(select id from actors where name='"+actor+"')))";
        return "";
    }

    @Override
    public String moreThan(String arg) {
        String year1 = arg.substring(0,4);
        String year2 = arg.substring(5,9);
        sql = "select count(*) as count1 from movies where year(release_time)='"+year1+"' and avg_score>2 ";
        int time1 = hive.getQuery(sql);
        sql = "select count(*) as count2 from movies where year(release_time)='"+year2+"' and avg_score>2 ";
        int time2 = hive.getQuery(sql);

        Results.HiveTime = time1 + time2;
        return "";
    }
}
