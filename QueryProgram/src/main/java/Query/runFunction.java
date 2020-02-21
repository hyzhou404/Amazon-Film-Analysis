package Query;

public class runFunction {
    private static String result;
    private static int argCheck(String arg, int num){
        if(arg.equals("") && num > 0){
            result = "参数不足";
            return -1;
        }
        String[] tmp = arg.split("#");
        if(tmp.length < num){
            result = "参数不足！";
            return -1;
        }
        else if(tmp.length > num){
            result = "参数过多！";
            return 1;
        }
        else{
            return 0;
        }
    }
    public static String run(String function, String arg){
        boolean hiveExe = false;
        switch (function) {
            case "列出前1000个电影":
                new mysqlQuery().showAllMovies(arg);
                result = new neo4jQuery().showAllMovies(arg);
                new influxQuery().showAllMovies(arg);
                if(hiveExe) {
                    new hiveQuery().showAllMovies(arg);
                }
                break;
            case "列出前1000个导演":
                new mysqlQuery().showAllDirectors(arg);
                result = new neo4jQuery().showAllDirectors(arg);
                new influxQuery().showAllDirectors(arg);
                if(hiveExe) {
                    new hiveQuery().showAllDirectors(arg);
                }
                break;
            case "列出前1000个演员":
                new mysqlQuery().showAllActors(arg);
                result = new neo4jQuery().showAllActors(arg);
                new influxQuery().showAllActors(arg);
                if(hiveExe) {
                    new hiveQuery().showAllActors(arg);
                }
                break;
            case "列出所有的电影类型":
                new mysqlQuery().showAllMovieTypes(arg);
                result = new neo4jQuery().showAllMovieTypes(arg);
                new influxQuery().showAllMovieTypes(arg);
                if(hiveExe) {
                    new hiveQuery().showAllMovieTypes(arg);
                }
                break;
            case "xxxx年xx月有多少部电影":
                if(argCheck(arg, 2)!=0){
                    break;
                }
                new mysqlQuery().ymMovieNum(arg);
                result = new neo4jQuery().ymMovieNum(arg);
                new influxQuery().ymMovieNum(arg);
                if(hiveExe) {
                    new hiveQuery().ymMovieNum(arg);
                }
                break;
            case "xxxx电影有多少版本":
                if(argCheck(arg, 1)!=0){
                    break;
                }
                new mysqlQuery().movieEditionNum(arg);
                result = new neo4jQuery().movieEditionNum(arg);
                new influxQuery().movieEditionNum(arg);
                if(hiveExe) {
                    new hiveQuery().movieEditionNum(arg);
                }
                break;
            case "xxx导演共有多少部电影":
                if(argCheck(arg, 1)!=0){
                    break;
                }
                new mysqlQuery().movieByDirectorNum(arg);
                result = new neo4jQuery().movieByDirectorNum(arg);
                new influxQuery().movieByDirectorNum(arg);
                if(hiveExe) {
                    new hiveQuery().movieByDirectorNum(arg);
                }
                break;
            case "xxx演员主演了多少部电影":
                if(argCheck(arg, 1)!=0){
                    break;
                }
                new mysqlQuery().movieMainByActorNum(arg);
                result = new neo4jQuery().movieMainByActorNum(arg);
                new influxQuery().movieMainByActorNum(arg);
                if(hiveExe) {
                    new hiveQuery().movieMainByActorNum(arg);
                }
                break;
            case "xxx演员参演了多少部电影":
                if(argCheck(arg, 1)!=0){
                    break;
                }
                new mysqlQuery().movieByActorNum(arg);
                result = new neo4jQuery().movieByActorNum(arg);
                new influxQuery().movieByActorNum(arg);
                if(hiveExe) {
                    new hiveQuery().movieByActorNum(arg);
                }
                break;
            case "xxx类型的电影共有多少部":
                if(argCheck(arg, 1)!=0){
                    break;
                }
                new mysqlQuery().typeNum(arg);
                result = new neo4jQuery().typeNum(arg);
                new influxQuery().typeNum(arg);
                if(hiveExe) {
                    new hiveQuery().typeNum(arg);
                }
                break;
            case "xxx演员经常和哪些演员合作":
                if(argCheck(arg, 1)!=0){
                    break;
                }
                new mysqlQuery().frequentActors(arg);
                result = new neo4jQuery().frequentActors(arg);
                new influxQuery().frequentActors(arg);
                if(hiveExe) {
                    new hiveQuery().frequentActors(arg);
                }
                break;
            case "xxx演员经常和哪些导演合作":
                if(argCheck(arg, 1)!=0){
                    break;
                }
                new mysqlQuery().frequentDirectors(arg);
                result = new neo4jQuery().frequentDirectors(arg);
                new influxQuery().frequentDirectors(arg);
                if(hiveExe) {
                    new hiveQuery().frequentDirectors(arg);
                }
                break;
            case "xxx类型电影常见的黄金上映期是什么":
                if(argCheck(arg, 1)!=0){
                    break;
                }
                new mysqlQuery().typeBestTime(arg);
                result = new neo4jQuery().typeBestTime(arg);
                new influxQuery().typeBestTime(arg);
                if(hiveExe) {
                    new hiveQuery().typeBestTime(arg);
                }
                break;
            case "xxx类型电影在哪天上映的最多":
                if(argCheck(arg, 1)!=0){
                    break;
                }
                new mysqlQuery().mostDayForType(arg);
                result = new neo4jQuery().mostDayForType(arg);
                new influxQuery().mostDayForType(arg);
                if(hiveExe) {
                    new hiveQuery().mostDayForType(arg);
                }
                break;
            case "xxx演员参演的所有有xxx语言版本的电影":
                if(argCheck(arg, 2)!=0){
                    break;
                }
                new mysqlQuery().languageByActor(arg);
                result = new neo4jQuery().languageByActor(arg);
                new influxQuery().languageByActor(arg);
                if(hiveExe) {
                    new hiveQuery().languageByActor(arg);
                }
                break;
            case "xxxx年比xxxx年多多少部评价在2星以上的电影":
                if(argCheck(arg, 2)!=0){
                    break;
                }
                new mysqlQuery().moreThan(arg);
                result = new neo4jQuery().moreThan(arg);
                new influxQuery().moreThan(arg);
                if(hiveExe) {
                    new hiveQuery().moreThan(arg);
                }
                break;
        }
        return result;
    }
}
