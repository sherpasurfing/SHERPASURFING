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
// utilized
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

// cloudera/apache base
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
// google common
import com.google.common.base.Preconditions;


public class sink extends EventSink.Base {
  private PrintWriter pw;
  
  @Override
  public void open() throws IOException {
    Date date = new Date();
    String s = null;
    // data is written to files with date format  
    // is is intended to be written several times an hour (20 minute rwcuts)
    DateFormat df = new SimpleDateFormat("MMddyyyyHHmm");
    s = df.format(date);
    pw = new PrintWriter(new FileWriter(s+".netflow"));
  }

  @Override
  public void append(Event e) throws IOException {  
    // just the body
    pw.println(new String(e.getBody()));
    pw.flush(); 
  }

  @Override
  public void close() throws IOException {
    // Cleanup
    pw.flush();
    pw.close();
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      // construct a new parameterized sink
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length == 0, "usage: Netflow Sink");
        return new sink();
      }
    };
  }
/**-----------------------------------------------------------**/
  public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
    List<Pair<String, SinkBuilder>> builders =
      new ArrayList<Pair<String, SinkBuilder>>();
    builders.add(new Pair<String, SinkBuilder>("Decorator", builder()));
    return builders;
  }
}
