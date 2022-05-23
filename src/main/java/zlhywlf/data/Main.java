package zlhywlf.data;

import zlhywlf.data.word.WordCountDriver;

/**
 * @author zlhywlf
 */
public class Main {

    public static void main(String[] args) {
        // 计算
        if (args.length == 2) {
            WordCountDriver.driver(args[0], args[1]);
        } else {
            WordCountDriver.driver("/collect_log_2022-05-20", "/users8");
        }
    }

}