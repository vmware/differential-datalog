import org.apache.catalina.Context;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.LifecycleException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.File;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlog.reach.*;
import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import org.apache.tomcat.util.descriptor.web.FilterDef;
import org.apache.tomcat.util.descriptor.web.FilterMap;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;

// This program is a bit ugly with all these static variables, but it
// fits in one single file.
public class Main {
    // Describes a graph change: edge insertion or deletion
    public static class Change {
        final boolean insert;
        final int source;
        final int destination;

        public Change(boolean insert, int source, int destination) {
            this.insert = insert;
            this.source = source;
            this.destination = destination;
        }
    }

    // A graph with N nodes
    public static class Graph {
        static final int N = 20;
        // We only generate edges s.t. source < dest.
        final boolean[][] edges;
        public final Random rnd;

        public Graph() {
            this.edges = new boolean[N][N];
            this.rnd = new Random(1);
        }

        public List<Change> initialChanges() {
            List<Change> result = new ArrayList<Change>(N);
            for (int i=0; i < N; i++)
                result.add(new Change(true, i, i));
            return result;
        }

        public Change generateChange(boolean insert) {
            for (int retry = 0; retry < 2 * Graph.N; retry++) {
                int src = this.rnd.nextInt(Graph.N);
                int dest = this.rnd.nextInt(Graph.N);
                if (src == dest)
                    dest = (dest + 1) % Graph.N;
                if (src > dest) {
                    int tmp = src;
                    src = dest;
                    dest = tmp;
                }

                if (insert != this.edges[src][dest]) {
                    Change result = new Change(insert, src, dest);
                    this.edges[src][dest] = insert;
                    return result;
                }
            }
            return null;
        }
    }

    // Store here outputs received from DDlog but not yet sent to browser
    private static StringBuilder fromDDlog = new StringBuilder().append("[");
    private static boolean first = true;

    // Get the latest changes that were received but not yet sent to the client
    synchronized
    private static String getChange() {
        fromDDlog.append("]");
        String result = fromDDlog.toString();
        fromDDlog.setLength(0);
        fromDDlog.append("[");
        first = true;
        return result;
    }

    // A row of the "Edges" relation in DDlog
    public static class Edges {
        public int source;
        public int dest;

        public String toJson() {
            return quote("table") + ": " + quote("Edges") + ", " +
                    quote("source") + ": " + this.source + ", " +
                    quote("dest") + ": " + this.dest;
        }
    }

    // A row of the "Canonical" relation in DDlog
    public static class Canonical {
        public int node;
        public int repr;

        public String toJson() {
            return quote("table") + ": " + quote("Canonical") + ", " +
                    quote("node") + ": " + this.node + ", " +
                    quote("repr") + ": " + this.repr;
        }
    }

    public static String quote(String s) {
        return "\"" + s + "\"";
    }

    // Upcall invoked by DDlog commit
    synchronized
    private static void onCommit(DDlogCommand command) {
        // Constructs a Json string that will be sent by the web server
        // to the client.
        try {
            if (!Main.first) {
                fromDDlog.append(",\n");
            } else {
                Main.first = false;
            }
            boolean insert = command.kind == DDlogCommand.Kind.Insert;
            fromDDlog.append("{ " + quote("insert") + ": " + insert + ", ");
            if (Main.ddlog.isCanonical(command.table)) {
                Canonical c = command.getValue(Canonical.class);
                fromDDlog.append(c.toJson());
            } else {
                Edges e = command.getValue(Edges.class);
                fromDDlog.append(e.toJson());
            }
            fromDDlog.append(" }");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // Interact with DDlog computation
    public static final class DDlog {
        private final DDlogAPI api;
        reachUpdateBuilder builder;

        public DDlog() {
            this.api = new DDlogAPI(1, null, false);
        }

        void start() {
            this.api.start();
            this.builder = new reachUpdateBuilder();
        }

        public void changeEdge(Change c) {
            if (c.insert)
                this.builder.insert_Edge((short)c.source, (short)c.destination);
            else
                this.builder.delete_Edge((short)c.source, (short)c.destination);
        }

        void commit() {
            int res = this.builder.applyUpdates(this.api);
            if (res != 0)
                throw new RuntimeException("Error when applying updates");
            this.api.commit_dump_changes(r -> Main.onCommit(r));
        }

        void stop() {
            this.api.stop();
        }

        // True if this table is the "Canonical" table.
        public boolean isCanonical(int tableId) {
            return this.api.getTableId("Canonical") == tableId;
        }
    }

    // Make a set of random changes in the graph.
    public static int counter = 0;
    public static void randomChanges() {
        Main.ddlog.start();
        for (int i = 0; i < 1 + Main.graph.rnd.nextInt(Graph.N / 5); i++) {
            Change c = graph.generateChange(Main.counter % 2 == 0);
            if (c != null)
                ddlog.changeEdge(c);
        }
        Main.ddlog.commit();
        Main.counter++;
    }

    // Embedded servlet for url "g"
    public static class Servlet extends HttpServlet {
        // On every invocation returns the new outputs received from DDlog
        // since the last invocation.
        @Override
        protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
            response.setContentType("text/html");
            PrintWriter w = response.getWriter();
            String updates = Main.getChange();
            w.write(updates);
            // Make more random graph changes
            Main.randomChanges();
        }
    }

    // Embedded servlet for url "index.html"
    public static class Index extends HttpServlet {
        @Override
        protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
            File file = new File(".", "index.html");
            response.setContentType("text/html");
            response.setContentLength((int)file.length());
            Files.copy(file.toPath(), response.getOutputStream());
        }
    }

    public static DDlog ddlog = new DDlog();
    public static Graph graph;

    public static void main(String[] args)
            throws LifecycleException, InterruptedException, ServletException {
        Tomcat tomcat = new Tomcat();
        tomcat.setPort(8082);

        // generate initial graph
        Main.ddlog.start();
        Main.graph = new Graph();
        // add self-edges
        for (Change c : graph.initialChanges())
            ddlog.changeEdge(c);
        Main.ddlog.commit();

        // Register servlets
        Context ctx = tomcat.addContext("/", new File(".").getAbsolutePath());
        tomcat.addServlet(ctx, "Embedded", new Servlet());
        ctx.addServletMapping("/g", "Embedded");          // Url is g
        tomcat.addServlet(ctx, "Index", new Index());
        ctx.addServletMapping("/index.html", "Index");

        // Run the web server
        tomcat.start();
        tomcat.getServer().await();  // Not clear if this ever returns
        Main.ddlog.stop();
    }
}
