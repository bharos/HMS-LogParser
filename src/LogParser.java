import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * Accepts 3 command-line arguments :
 *      1. Path to HMS log file (Required).
 *      2. Path to csv file where parsed log information need to be written to (Required).
 *      3. Boolean indicating whether to filter methods that do not create notifications (Optional, default=false).
 * Parses the HMS log for PerfLogger entries, and
 * obtains information about method name and timestamps from the perflogger entries,
 * Writes this information too a CSV file. Then, it filters the rows in CSV based on Strings
 * added in filterMethods to get only the relevant API calls.
 * Now, sorts the entries and finds the maximum concurrency noticed in the logs
 * based on the start and end times of API calls for each method.
 */
public class LogParser {

  private static List<String> filterMethods = new ArrayList<String>() {{
      add("method=get");
      add("method=shutdown");
    }};


   // Map which holds all the method names and their frequency
  private static Map<String, Integer> methodCountMap = new HashMap<>();

  private static Comparator<LogElement> timeComparator = Comparator.comparingLong(LogElement::getTime);
  // Holds the set of start and end times in a sorted order
  private static List<LogElement> times = new ArrayList<>();

  private static boolean shouldFilter;
  //  private static PrintWriter pw;
  public static void main(String[] args) {

    String logFile = args[0];
    String csvFile = args[1];
    if (args.length == 3 && args[2] != null) {
      shouldFilter = Boolean.parseBoolean(args[2]);

    }
    String line;
    String splitBy = ",";

    createCsv(logFile, csvFile);


    try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

      while ((line = br.readLine()) != null) {

        String[] parts = line.split(splitBy);

        //Parse the log elements from current line
        String method = parts[3];
        long startTime = Long.parseLong(parts[4].split("=")[1]);
        long endTime = Long.parseLong(parts[5].split("=")[1]);
        long duration = Long.parseLong(parts[6].split("=")[1]);

        LogElement startElement = new LogElement(method, startTime, duration, true);
        LogElement endElement = new LogElement(method, endTime, duration, false);
        times.add(startElement);
        times.add(endElement);

        methodCountMap.put(method, methodCountMap.getOrDefault(method, 0) + 1);

      }
      printLogMap(methodCountMap);

      if (shouldFilter) {
        times = filterTimes();
      }

      int maxThreads = findMaxConcurrency();
      System.out.println("\n\n\n");
      System.out.println("Maximum concurrency : " + maxThreads);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void createCsv(String logFile, String csvFile) {
    File file =new File(logFile);
    Scanner in;
    PrintWriter printWriter;
    try {
      printWriter = new PrintWriter(new File(csvFile));

      in = new Scanner(file);
      while(in.hasNext()) {
        String line = in.nextLine();
        if (line.contains("</PERFLOG method=")) {
          String[] parts = line.split("\\s+");
          StringBuilder sb = new StringBuilder();

          sb.append(parts[0]);
          sb.append(",");
          sb.append(parts[1]);
          sb.append(",");
          sb.append(parts[6]);
          sb.append(",");
          sb.append(parts[7]);
          sb.append(",");
          sb.append(parts[8]);
          sb.append(",");
          sb.append(parts[9]);
          sb.append(",");
          sb.append("\n");
          printWriter.write(sb.toString());
        }
      }
      printWriter.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  private static List<LogElement> filterTimes() {
    return times.stream().filter((LogElement logElement) -> {
        for (String f : filterMethods) {
          if (logElement.getMethodName().contains(f)) {
            return false;
          }
        }
        return true;
      }).collect(Collectors.toList());
  }

  private static int findMaxConcurrency() {
    Map<String, Integer> methodMap = new HashMap<>();
    Map<String, Integer> maxMap = new HashMap<>();

    int currentThreads = 0, maxThreads = 0;

    //Sort the times list
    times.sort(timeComparator);

    for (LogElement t : times) {
      if (t.isStart()) {
        currentThreads++;

        methodMap.put(t.getMethodName(), methodMap.getOrDefault(t.getMethodName(), 0) + 1);

//        printCurrentThreadAndMethodsCsv(currentThreads, methodMap);

        if (currentThreads > maxThreads) {
          maxThreads = currentThreads;
          maxMap = new HashMap<>(methodMap);
        }
      } else {
        currentThreads--;
        int count = methodMap.get(t.getMethodName());
        count--;
        if (count == 0) {
          methodMap.remove(t.getMethodName());
        } else {
          methodMap.put(t.getMethodName(), count);
        }

      }
    }
    printLogMap(maxMap);
//    pw.close();
    return maxThreads;
  }

/*
  private static void printCurrentThreadAndMethodsCsv(int currentThreads, Map<String, Integer> methodMap) {
    if (pw == null) {
      try {
        pw = new PrintWriter(new File("/Users/bharathkrishna/hms_log_count_parsed.csv"));
        pw.write("THREAD_COUNT,METHODS\n");
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
    }
    StringBuilder sb = new StringBuilder();
    sb.append(currentThreads);
    sb.append(",");
    sb.append(getLogMapCsv(methodMap));
    sb.append("\n");
    pw.write(sb.toString());

  }

  private static String getLogMapCsv(Map<String, Integer> map) {
    String delim = "";
    StringBuilder sb = new StringBuilder();
    return String.valueOf(map.size());
//    for (Map.Entry<String, Integer> entry : map.entrySet()) {
//      String method = entry.getKey().split("=")[1];
//      int count = entry.getValue();
//
//      sb.append(delim);
//      sb.append(method);
//      sb.append(":");
//      sb.append(count);
//      delim=",";
//    }
//    return sb.toString();
  }
  */
  private static void printLogMap(Map<String, Integer> map) {
    System.out.println("\n\nMAP OF METHOD NAME WITH FREQUENCY");
    for (Map.Entry<String, Integer> entry : map.entrySet()) {
      System.out.print(entry.getKey());
      System.out.print(" : " + entry.getValue());
      System.out.println();
    }
  }
}


class LogElement {
  private long time;
  private boolean isStart;
  private String methodName;
  private long duration;

  public LogElement(String methodName, long time, long duration, boolean isStart) {
    this.methodName = methodName;
    this.time = time;
    this.duration = duration;
    this.isStart = isStart;
  }

  public long getTime() {
    return time;
  }

  public long getDuration() {
    return duration;
  }
  public String getMethodName() {
    return methodName;
  }

  public boolean isStart() {
    return isStart;
  }

  @Override
  public String toString() {
    return methodName + " Time : " + time + " Duration : " + duration + " Start : " + isStart;
  }
}

