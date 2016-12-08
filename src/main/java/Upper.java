import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * Created by hadoop on 8/12/16.
 */
public class Upper extends EvalFunc<Tuple> {


    public Tuple exec(Tuple tuple) throws IOException {

        Tuple myTuple = null;
        try {
            String firstName = tuple.get(2).toString();
            String lastName = tuple.get(3).toString();

            firstName = firstName.toUpperCase();
            lastName = lastName.toUpperCase();

            myTuple = TupleFactory.getInstance().newTuple();
            myTuple.append(firstName);
            myTuple.append(lastName);
        }
        catch(Exception e){
            throw new RuntimeException("Error processing tuple " + tuple.get(2).toString() +", "+ tuple.get(3).toString());
        }
        return myTuple;
    }
}