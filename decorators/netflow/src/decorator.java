///--------------------------------------------------------------------
///
///  SHERPASURFING is an open source solution. The components  that are provided
///  are intended to aid in the development of a Cyber Security Solution at no
///  cost.
///
///    Copyright (C) 2010 - 2016  Wayne wheeles aka "SHERPA"
///
///  This program is free software: you can redistribute it and/or modify
///  it under the terms of the GNU General Public License as published by
///  the Free Software Foundation, either version 3 of the License, or
///  (at your option) any later version.
///
///  This program is distributed in the hope that it will be useful,
///  but WITHOUT ANY WARRANTY; without even the implied warranty of
///  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
///  GNU General Public License for more details.
///
///  You should have received a copy of the GNU General Public License
///  along with this program.  If not, see <http://www.gnu.org/licenses/>.
///
///--------------------------------------------------------------------

package netflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
//------------------------/
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;
//----------------------- /
// SHERPA COMMON
import com.sherpasurfing.common.*;
// ----------------------/

/**************************************************************
 * The intent is to provide a generic netflow decorator 
 * which can be adapted to any use
 * the standard delimiter for all applications in sherpa is |
 * which can be changed if required by setting the "delimiter"
 * @author SHERPA
 *************************************************************/
public class decorator<S extends EventSink>
    extends EventSinkDecorator<S>
{     
    public decorator(S s)
    {
        super(s);
    }

    @Override
    public void append(Event e) throws IOException
    {
        long sip = 0;
        long dip = 0;
        // THE RWCUT or Silk feed is based on the profile or map below
        // if using the rwcut for silk provided this will be the field map
        // we are adding/enriching using a utility so that the dot notation
        // will be converted to a number for sip and dip
        //----------------------------------------------------------------
        // FIELD MAP, Field Number
        // sIP| 0            
        // dIP| 1
        // sPort| 2
        // dPort| 3
        // pro|   4
        // packets| 5     
        // bytes|   6
        // flags|   7               
        // sTime|   8   
        // dur|     9             
        // eTime|   10
        // sen|     11
        // in|  12
        // out|  13 
        // type| 14
        // iTy|  15
        // iCo|  16
        // initialF|  17
        // sessionF|  18
        // attribut|  19
        // appli|     20
//----------------------------------------------------------------        
//   
        String[] body = new String(e.getBody()).split("\\|");
        for (int i = 0; i < body.length; i++)
        {
            // trim the whitespaces off of the values
            body[i] = body[i].trim();
        }
        //String key = body[0] + "_" + body[1] + "_" + body[2] + "_"
        //           + body[3] + "_" + body[8];
        // this is the sip and the dip for the event 
        sip = Utilities.StringIPtoInteger(body[0]);
        dip = Utilities.StringIPtoInteger(body[1]);        
        
           int key = (body[0] + "~" + body[1]).hashCode();
        
        // get rid of characters added to netflow
        // key = key.replaceAll("\\/", "");

        StringBuilder newData = new StringBuilder(key);
            newData.append(sip);
            newData.append("\\|");
            newData.append(dip);
            
        for (int i = 0; i < body.length; i++)
        {
            newData.append("\\|");
            newData.append(body[i]);
        }
        EventImpl e2 = new EventImpl(newData.toString().getBytes(),
                                     e.getTimestamp(), e.getPriority(),
                                     e.getNanos(), e.getHost(),
                                     new HashMap(e.getAttrs()));
        try
        {
            super.append(e2);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }
    public static SinkDecoBuilder builder()
    {
        return new SinkDecoBuilder()
        {
            @Override
            public EventSinkDecorator<EventSink> build(Context context,
                                                       String... argv)
            {
                String usage = "usage: NetflowDecorator based on ascii file generated by SILK rwcut";
                Preconditions.checkArgument(argv.length == 0, usage);
                return new decorator<EventSink>(null);
            }
        };
    }

    public static List<Pair<String, SinkDecoBuilder>> getDecoratorBuilders()
    {
        List<Pair<String, SinkDecoBuilder>> builders =
            new ArrayList<Pair<String, SinkDecoBuilder>>();
        builders.add(new Pair<String, SinkDecoBuilder>("Decorator",
                                                       builder()));
        return builders;
    }
}
