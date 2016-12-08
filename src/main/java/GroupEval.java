import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by hadoop on 8/12/16.
 */
public class GroupEval extends EvalFunc<Tuple> {


    public Tuple exec(Tuple tuple) throws IOException {

        String deptId = tuple.get(0).toString();
        DataBag bag = (DataBag)tuple.get(1);
        Iterator it = bag.iterator();
        double max = 0;
        while(it.hasNext()){
            Tuple salaryTuple = (Tuple)it.next();

            Double temp = (Double)salaryTuple.get(1);
            if(temp>max){
                max = temp;
            }
        }
        Tuple myTuple = TupleFactory.getInstance().newTuple();
        myTuple.append(deptId);
        myTuple.append(max);

        return myTuple;
    }
}
