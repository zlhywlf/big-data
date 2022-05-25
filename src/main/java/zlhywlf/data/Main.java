package zlhywlf.data;

import zlhywlf.data.group.GroupDriver;
import zlhywlf.data.partition.PartitionDriver;
import zlhywlf.data.sort.SortDriver;
import zlhywlf.data.speak.SpeakDriver;
import zlhywlf.data.word.WordCountDriver;

/**
 * @author zlhywlf
 */
public class Main {

    public static void main(String[] args) {
        // 计算
        if (args.length == 2) {
            GroupDriver.driver(args[0], args[1]);
        } else {
            GroupDriver.driver("logs/groupingComparator.log", "logs/test");
        }
    }

}