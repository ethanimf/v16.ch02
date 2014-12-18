package ethan.v16.ch02.mr.kpi;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.hdfs.server.namenode.GetDelegationTokenServlet;

/**
 * @since 2014-9-20
 * @author ethan
 */

public class MyKPI {

	String remote_addr; // 58.215.204.118
	String remote_user;
	String time_local; // [18/Sep/2013:06:51:36 +0000]
	String request; // GET wp-includes/js/jquery/..
	String status; // 304
	String body_bytes_sent; // 0
	String http_referer; // "http://blog.fens.me/
	String http_user_agent;
	boolean isValid = true;

	public boolean isValid() {
		return isValid;
	}

	public void setValid(boolean isValid) {
		this.isValid = isValid;
	}

	public String getRemote_addr() {
		return remote_addr;
	}

	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}

	public String getRemote_user() {
		return remote_user;
	}

	public void setRemote_user(String remote_user) {
		this.remote_user = remote_user;
	}

	public String getTime_local() {
		return time_local;
	}

	public void setTime_local(String time_local) {
		this.time_local = time_local;
	}

	public String getRequest() {
		return request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}

	public void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}

	public String getHttp_referer_demain() {
//		return http_referer;
		if(http_referer.length()<8){
			return http_referer;
		}
		String str = http_referer.replace("\"", "").replace("http://", "").replace("https://", "");
		return str.indexOf("/")>0?str.substring(0, str.indexOf("/")):str;
	}

	public void setHttp_referer(String http_referer) {
		this.http_referer = http_referer;
	}

	public String getHttp_user_agent() {
		return http_user_agent;
	}

	public void setHttp_user_agent(String http_user_agent) {
		this.http_user_agent = http_user_agent;
	}

	public static void main(String[] args) throws ParseException {
		String line = "50.116.27.194 - - [18/Sep/2013:06:51:35 +0000] \"POST /wp-cron.php?doing_wp_cron=1379487095.2510800361633300781250 HTTP/1.0\" 200 0 \"-\" \"WordPress/3.6; http://blog.fens.me\"";
		System.out.println(line);
		MyKPI kpi = new MyKPI();
		String[] arr = line.split(" ");
		kpi.setRemote_addr(arr[0]);
		kpi.setTime_local(arr[3].substring(1));
		kpi.setRequest(arr[6]);
		kpi.setStatus(arr[8]);
		kpi.setBody_bytes_sent(arr[9]);
		kpi.setHttp_referer(arr[10]);
		kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
		System.out.println(kpi.toString());
		System.out.println(kpi.getTime_local_Date_hour());
		System.out.println(kpi.getTime_local_Date());
		System.out.println(kpi.getHttp_referer_demain());
	}

	/**
	 * @param line
	 * @return
	 */
	public static MyKPI parserKPI(String line) {
		String[] arr = line.split(" ");
		MyKPI kpi = new MyKPI();
		if (arr.length > 11) {
			kpi.setRemote_addr(arr[0]);
			kpi.setTime_local(arr[3].substring(1));
			kpi.setRequest(arr[6]);
			kpi.setStatus(arr[8]);
			kpi.setBody_bytes_sent(arr[9]);
			kpi.setHttp_referer(arr[10]);
			if (arr.length > 12) {
				kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
			} else {
				kpi.setHttp_user_agent(arr[11]);
			}
			try{
				if (Integer.parseInt(kpi.getStatus()) >= 400) { // HTTP ERROR
					kpi.setValid(false);
				}
			}catch (NumberFormatException e) {
				kpi.setValid(false);
			}

		} else {
			kpi.setValid(false);
		}

		return kpi;
	}

	@Override
	public String toString() {

		// return remote_addr +"\t"+ time_local +"\t"+ request +"\t"+ status
		// +"\t"+ body_bytes_sent +"\t"+http_referer+"\t"+http_user_agent;
		StringBuffer sb = new StringBuffer();
		sb.append("isValid:" + isValid).append("\nremote_addr:" + remote_addr).append("\ntime_local:" + time_local).append("\nrequest:" + request).append("\nstatus:" + status)
				.append("\nbody_bytes_sent:" + body_bytes_sent).append("\nhttp_referer:" + http_referer).append("\nhttp_user_agent:" + http_user_agent);
		return sb.toString();

	}

	public static MyKPI filterPVs(String line) {
		MyKPI kpi = parserKPI(line);
		Set<String> pages = new HashSet<String>();
		pages.add("/wp-cron.php?doing_wp_cron=1379487095.2510800361633300781250");
		pages.add("/about/");
		pages.add("/black-ip-list/");
		pages.add("/cassandra-clustor/");
		pages.add("/finance-rhive-repurchase/");
		if (!pages.contains(kpi.getRequest())) {
			kpi.setValid(false);
		}
		return kpi;
	}
	
	public static MyKPI filterBrowser(String line) {
		return parserKPI(line);
	}
	public static MyKPI filterSource(String line) {
		return parserKPI(line);
	}
	public static MyKPI filterTime(String line) {
		return parserKPI(line);
	}
	
	public static MyKPI filterIPs(String line){
		MyKPI kpi = parserKPI(line);
		Set<String> pages = new HashSet<String>();
		pages.add("/wp-cron.php?doing_wp_cron=1379487095.2510800361633300781250");
		pages.add("/about/");
		pages.add("/black-ip-list/");
		pages.add("/cassandra-clustor/");
		pages.add("/finance-rhive-repurchase/");
		if (!pages.contains(kpi.getRequest())) {
			kpi.setValid(false);
		}
		return kpi;
	}
	public  Date getTime_local_Date() throws ParseException{
//		18/Sep/2013:06:51:35
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss",Locale.US);
		return sdf.parse(this.time_local);
	}
	
	public String getTime_local_Date_hour() throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhh");
		return sdf.format(getTime_local_Date());
	}
}
