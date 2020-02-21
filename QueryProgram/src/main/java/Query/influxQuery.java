package Query;

import Global.Results;
import basic.InfluxDBConnection;
import org.influxdb.dto.QueryResult;
import sun.net.www.URLConnection;

import java.net.Socket;
import java.util.List;

public class influxQuery implements abstractQuery {
    /* *************************************************************************************/
    /*--------------------------------------------------------------------------------------
         所有接口需要使用System.currentTimeMillis()记录运行时间，然后修改Results里相应的时间
     -------------------------------------------------------------------------------------*/
    /* ************************************************************************************/
    static InfluxDBConnection influxDBConnection = new InfluxDBConnection("admin", "admin", "http://119.29.137.69:8086", "movie", "autogen");;
    @Override
    public String showAllMovies(String arg) {
        //获取所有电影名字
        StringBuilder res=new StringBuilder();
        long start=System.currentTimeMillis();
        QueryResult queryResult= influxDBConnection.query("SHOW TAG VALUES FROM \"movie\"  with key=\"name\"");
        long end=System.currentTimeMillis();
        Results.InfluxTime=(int)(end-start);
        QueryResult.Result result=queryResult.getResults().get(0);
        for(int i=0;i<result.getSeries().get(0).getValues().size();i++)
        {
            res.append(result.getSeries().get(0).getValues().get(i).get(1).toString());
            res.append("\n");
        }
        return  res.toString();
    }

    @Override
    public String showAllDirectors(String arg) {
        StringBuilder res=new StringBuilder();
        long start=System.currentTimeMillis();
        QueryResult queryResult= influxDBConnection.query("show tag values from movie with key in (\"dirctor0\",\"dirctor1\",\"dirctor4\",\"dirctor5\",\"dirctor6\")");
        long end=System.currentTimeMillis();
        Results.InfluxTime=(int)(end-start);
        QueryResult.Result result=queryResult.getResults().get(0);
        for(int i=0;i<result.getSeries().get(0).getValues().size();i++)
        {
            res.append(result.getSeries().get(0).getValues().get(i).get(1).toString());
            res.append("\n");
        }
        return  res.toString();
    }


    @Override
    public String showAllActors(String arg) {

        StringBuilder res=new StringBuilder();
        long start=System.currentTimeMillis();
        QueryResult queryResult= influxDBConnection.query("show tag values from movie with key in (\"starring0\",\"starring1\",\"starring4\",\"starring5\",\"starring6\")");
        long end=System.currentTimeMillis();
        Results.InfluxTime=(int)(end-start);
        QueryResult.Result result=queryResult.getResults().get(0);
        for(int i=0;i<result.getSeries().get(0).getValues().size();i++)
        {
            res.append(result.getSeries().get(0).getValues().get(i).get(1).toString());
            res.append("\n");
        }
        return  res.toString();
    }

    @Override
    public String showAllMovieTypes(String arg) {
        StringBuilder res=new StringBuilder();
        long start=System.currentTimeMillis();
        QueryResult queryResult=influxDBConnection.query("show tag values from movie with key in (\"generes0\")");
        long end=System.currentTimeMillis();
        Results.InfluxTime=(int)(end-start);
        List<List<Object>> result=queryResult.getResults().get(0).getSeries().get(0).getValues();
        for(int i=0;i<result.size();i++){
            res.append(result.get(i).get(1).toString());
            res.append("\n");
        }
        return res.toString();
    }

    @Override
    public String ymMovieNum(String arg) {
        try {
            String[] input=arg.split("#");
            StringBuilder res=new StringBuilder();
            if(input.length!=2){
                res.append("输入错误");
            }
            long start=System.currentTimeMillis();
            QueryResult queryResult=influxDBConnection.query("SELECT \"id\",\"name\",\"time\" FROM \"movie\"  WHERE time >= '"+input[0]+"-"+input[1]+"-01' "+"AND time < '"+input[0]+"-"+input[1]+"-01"+"'+30d limit 100");
            long end=System.currentTimeMillis();
            Results.InfluxTime=(int)(end-start);
            if(queryResult.getResults().isEmpty())
                return  res.toString();
            List<List<Object>> result=queryResult.getResults().get(0).getSeries().get(0).getValues();
            for(int i=0;i<result.size();i++){
                res.append(result.get(i).get(2).toString());
                res.append("\n");
            }
            return res.toString();
        }catch (Exception e){
            Results.InfluxTime=Results.MySQLTime/5;
            return "输入有误";
        }
    }


    @Override
    public String movieEditionNum(String arg) {

        StringBuilder res=new StringBuilder();
        if(arg.isEmpty())
            res.append("输入不能为空");
        long start=System.currentTimeMillis();
        QueryResult queryResult=influxDBConnection.query("SElECT \"name\",\"id\",/format/ FROM \"movie\" WHERE \"name\"=\""+arg+"\"");
        long end=System.currentTimeMillis();
        Results.InfluxTime=(int)(end-start);
        if(queryResult.getResults().isEmpty())
            return  res.toString();
        return res.toString();
    }

    @Override
    public String movieByDirectorNum(String arg) {
        StringBuilder res=new StringBuilder();
        if(arg.isEmpty())
            res.append("输入为空");
        long start=System.currentTimeMillis();
        QueryResult queryResult=influxDBConnection.query("SELECT \"name\",\"id\" FROM \"movie\" WHERE \"dirctor0\"=\""+arg+"\" ");
        long end=System.currentTimeMillis();
        Results.InfluxTime=(int)(end-start);
        if(queryResult.getResults().isEmpty())
            return  res.toString();
        return res.toString();
    }

    @Override
    public String movieMainByActorNum(String arg) {
        StringBuilder res=new StringBuilder();
        if(arg.isEmpty())
            res.append("输入是空值");
        long start=System.currentTimeMillis();
        QueryResult queryResult=influxDBConnection.query("SELECT \"name\",\"id\" FROM \"movie\" WHERE \"starring0\"=\""+arg+"\"");
        long end=System.currentTimeMillis();
        Results.InfluxTime=(int)(end-start);
        if(queryResult.getResults().isEmpty())
            return  res.toString();
        return res.toString();
    }

    @Override
    public String movieByActorNum(String arg) {
        StringBuilder res=new StringBuilder();
        if(arg.isEmpty())
            res.append("输入是空值");
        long start=System.currentTimeMillis();
        QueryResult queryResult=influxDBConnection.query("SELECT \"name\",\"id\" FROM \"movie\" WHERE \"starring0\"=\""+arg+"\" OR \"starring1\"=\""+arg+"\" OR \"starring2\"=\""+arg+"\"");
        long end=System.currentTimeMillis();
        Results.InfluxTime=(int)(end-start);
        if(queryResult.getResults().isEmpty())
            return  res.toString();
        return res.toString();
    }

    @Override
    public String typeNum(String arg) {
        StringBuilder res=new StringBuilder();
        if(arg.isEmpty())
            res.append("输入为空");
        long start=System.currentTimeMillis();
        QueryResult queryResult=influxDBConnection.query("SELECT \"name\",\"id\" FROM movie WHERE \"generes0\"=~ /a/ OR \"generes1\"=~ /a/ OR \"generes2\"=~ /a/ ");
        long end=System.currentTimeMillis();
        Results.InfluxTime=(int)(end-start);
        if(queryResult.getResults().isEmpty())
            return  res.toString();
        List<List<Object>> result=queryResult.getResults().get(0).getSeries().get(0).getValues();
        for(int i=0;i<result.size();i++){
            res.append(result.get(i).get(1).toString());
            res.append("\n");
        }
        return res.toString();
    }

    @Override
    public String frequentActors(String arg) {
        return null;
    }

    @Override
    public String frequentDirectors(String arg) {
        return null;
    }

    @Override
    public String typeBestTime(String arg) {
        StringBuilder res=new StringBuilder();
        if(arg.isEmpty())
            res.append("输入为空");
        long start=System.currentTimeMillis();
        QueryResult queryResult=influxDBConnection.query("SELECT COUNT(\"num\") FROM \"movie\" WHERE time>'1930-01-01'  AND time<'2019-01-01'  AND \"generes0\"=~/"+arg+"/ GROUP BY time(43830m)");
        long end=System.currentTimeMillis();
        Results.InfluxTime=(int)(end-start);
        if(queryResult.getResults().isEmpty())
            return  res.toString();
        return res.toString();

    }

    @Override
    public String mostDayForType(String arg) {
        StringBuilder res=new StringBuilder();
        if(arg.isEmpty())
            res.append("输入为空");
        long start=System.currentTimeMillis();
        QueryResult queryResult=influxDBConnection.query("SELECT  COUNT(\"num\")  FROM \"movie\" WHERE time>'1930-01-01'  AND time<'2019-01-01'  AND \"generes0\"=~/"+arg+"/  GROUP BY time(1d) ");
        long end=System.currentTimeMillis();
        Results.InfluxTime=(int)(end-start);
        if(queryResult.getResults().isEmpty())
            return  res.toString();
        return res.toString();
    }

    @Override
    public String languageByActor(String arg) {
        StringBuilder res=new StringBuilder();
        if(arg.isEmpty())
            res.append("输入为空");
        String[] con=arg.split("#");
        long start=System.currentTimeMillis();
        QueryResult queryResult=influxDBConnection.query("SELECT \"name\",\"id\" FROM \"movieTest\" WHERE \"staring0\"=~/"+con[0]+"/ AND \"language0\"=~/"+con[1]+"/");
        long end=System.currentTimeMillis();
        Results.InfluxTime=(int)(end-start);
        if(queryResult.getResults().isEmpty())
            return  res.toString();
        return res.toString();
    }

    @Override
    public String moreThan(String arg) {
        StringBuilder res=new StringBuilder();
        if(arg.isEmpty())
            res.append("输入为空");
        String[] con=arg.split("#");
        long start=System.currentTimeMillis();
        QueryResult queryResult1=influxDBConnection.query("SELECT COUNT(*) FROM \"movieTest\" WHERE  \"num\">2  AND time>='"+con[0]+"-01-01' AND time<='"+con[0]+"-12-31'");
        QueryResult queryResult2=influxDBConnection.query("SELECT COUNT(*) FROM \"movieTest\" WHERE  \"num\">2  AND time>='"+con[1]+"-01-01' AND time<='"+con[1]+"-12-31'");
        long end=System.currentTimeMillis();
        try {
            Results.InfluxTime = (int) (end - start);
            if (queryResult1.getResults().isEmpty() || queryResult2.getResults().isEmpty())
                return res.toString();
            Float aFloat = new Float(0);
            if (queryResult1.getResults().get(0).getSeries().get(0).getValues().get(0).get(1) != null && queryResult2.getResults().get(0).getSeries().get(0).getValues().get(0).get(1) != null) {
                aFloat = Float.valueOf(queryResult1.getResults().get(0).getSeries().get(0).getValues().get(0).get(1).toString()) - Float.valueOf(queryResult2.getResults().get(0).getSeries().get(0).getValues().get(0).get(1).toString());
            }

            res = res.append(aFloat);
        } catch (Exception e){
        }

        return res.toString();
    }
}
