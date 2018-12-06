import java.io.*;
import java.net.Socket;
import java.util.*;
class Item{
    String key;
    String value;
    
}
class Page{
    long total_req_rate;
    int slab_no;
    List<String> items_ids;
    int max;
    Page(int slab,int m ){
        this.slab_no = slab;
        if(this.slab_no == 1) this.max = 10922;
        else if(this.slab_no == 2) this.max = 8738;
        else if(this.slab_no == 3) this.max = 6898;
        else if(this.slab_no == 4) this.max = 5461;
        else if(this.slab_no == 5) this.max = 4369;
        else if(this.slab_no == 6) this.max = 3449;
        else if(this.slab_no == 7) this.max = 2730;
        else if(this.slab_no == 8) this.max = 2184;
        else if(this.slab_no == 9) this.max = 1747;
        else if(this.slab_no == 10) this.max = 1394;
        else if(this.slab_no == 11) this.max = 1110;
        else if(this.slab_no == 12) this.max = 885;
        else if(this.slab_no == 13) this.max = 708;
        else if(this.slab_no == 14) this.max = 564;
        else if(this.slab_no == 15) this.max = 451;
        else if(this.slab_no == 16) this.max = 361;
        else if(this.slab_no == 17) this.max = 288;
        else if(this.slab_no == 18) this.max = 230;
        else if(this.slab_no == 19) this.max = 184;
        else if(this.slab_no == 20) this.max = 147;
        else if(this.slab_no == 21) this.max = 118;
        else if(this.slab_no == 22) this.max = 94;
        else if(this.slab_no == 23) this.max = 75;
        else if(this.slab_no == 24) this.max = 60;
        
        else System.out.print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSs");
        items_ids = new ArrayList<String>();
    }
}
class offline_tracer{
    static final int NUMBER_OF_SLABS = 32;
    private static int getSlabNo(String key){
        String key_size = key.length()>6?key.substring(0,key.length()-6):new String("0");
        if(Integer.parseInt(key_size) < 36) return 1;
        if(Integer.parseInt(key_size) < 54) return 2;
        if(Integer.parseInt(key_size) < 86) return 3; 
        if(Integer.parseInt(key_size) < 126) return 4;
        if(Integer.parseInt(key_size) < 174) return 5;
        if(Integer.parseInt(key_size) < 238) return 6; 
        
        if(Integer.parseInt(key_size) < 318) return 7;
        if(Integer.parseInt(key_size) < 414) return 8;
        
        if(Integer.parseInt(key_size) < 530) return 9;
        if(Integer.parseInt(key_size) < 690) return 10;

        if(Integer.parseInt(key_size) < 944-66) return 11;
        if(Integer.parseInt(key_size) < 1184) return 12;
        if(Integer.parseInt(key_size) < 1480-66) return 13;
        if(Integer.parseInt(key_size) < 1856-66) return 14;
        if(Integer.parseInt(key_size) < 2320-66) return 15;
        if(Integer.parseInt(key_size) < 2904-66) return 16;
        if(Integer.parseInt(key_size) < 3632-66) return 17;
        if(Integer.parseInt(key_size) < 4544-66) return 18;
        if(Integer.parseInt(key_size) < 5680-66) return 19;
        if(Integer.parseInt(key_size) < 7104-66) return 20;
        if(Integer.parseInt(key_size) < 8880-66) return 21;
         
        return 1; //Reimplement logic to get a slab number given a key (or should it be value)?????
    }
    private static HashMap sortByValues(HashMap map) { 
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
             public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o2)).getValue())
                   .compareTo(((Map.Entry) (o1)).getValue());
             }
        });
 
        HashMap sortedHashMap = new LinkedHashMap();
        for (Iterator it = list.iterator(); it.hasNext();) {
               Map.Entry entry = (Map.Entry) it.next();
               sortedHashMap.put(entry.getKey(), entry.getValue());
        } 
        return sortedHashMap;
    }
    public static HashMap rankItems(Item[] items){
        HashMap<String,Integer> key_rate_map = new HashMap<String,Integer>();
        for(Item i:items){
            key_rate_map.put(i.key,key_rate_map.getOrDefault(i.key,0)+1);
        }
        return sortByValues(key_rate_map);
    }

    public static List<Page> divideintopages(HashMap<String,Integer> sorted_rates){
        Page[] slab_page_map = new Page[NUMBER_OF_SLABS];
        List filled_pages_list = new ArrayList<Page>();
        for(Map.Entry<String,Integer> m:sorted_rates.entrySet()){
            int slab = getSlabNo(m.getKey());
            if(slab_page_map[slab] == null){
                slab_page_map[slab] = new Page(slab,64);
            }   
            slab_page_map[slab].items_ids.add(m.getKey());
            slab_page_map[slab].total_req_rate += m.getValue();
            if(slab_page_map[slab].items_ids.size() == slab_page_map[slab].max){
                filled_pages_list.add(slab_page_map[slab]);
                slab_page_map[slab] = null;
            }
        }
        for(int i=0;i<NUMBER_OF_SLABS;i++){
            if(slab_page_map[i]!=null){
            filled_pages_list.add(slab_page_map[i]);
            }
        }
        Collections.sort(filled_pages_list,new Comparator<Page>() {
           public int compare(Page a,Page b){
               return (int)(b.total_req_rate-a.total_req_rate);
           } 
        });
        return filled_pages_list;
    }
    public static HashMap<String,Integer> getSortedKeyDistribution(String file_path_for_key_trace){
        HashMap<String,Integer> map = new HashMap<String, Integer>();
        try{
            BufferedReader br = new BufferedReader(new FileReader(file_path_for_key_trace));
            String line =  null;
        
            while((line=br.readLine())!=null){     
                map.put(line, map.getOrDefault(line, 0)+1);
            }
            br.close();
            return sortByValues((HashMap)map);
        }catch(FileNotFoundException fe){
            fe.printStackTrace();
        }catch(IOException ie){
            ie.printStackTrace();
        }
        return null;
    }
    public static void sendOverTelnet(String server,int[] distribution){
        try{
            Socket telsoc = new Socket(server,11211);
            PrintWriter out = new PrintWriter(telsoc.getOutputStream(),true);
            String str = Arrays.toString(distribution).replaceAll("\\s+", "");
            str = str.replaceAll(","," ");
            out.println("slab_optimal "+str.substring(1,str.length()-1));
        }catch(IOException io){
            System.out.println(io.getMessage());
            return;
        }
    }
    public static void main(String[] args){
        /*
            Argument 1 - Serverlist.txt ==  List of memcached servers IP addresses
            Argument 2 - Folder path for key distribution file
                         File name should be mc1.txt or mc2.txt... etc indicating the memcached server.
            
        */
        while(true){
            File f = new File("~/Memcached_optimal_move/optimal/traces");
            if(f.list().length > 0){
            ArrayList<String> names = new ArrayList<String>(Arrays.asList(f.list()));
            for(String s:names){
                HashMap map = getSortedKeyDistribution("~/Memcached_optimal_move/optimal/traces"+s);
                List<Page> final_list = divideintopages(sortByValues((HashMap)map));
                
                int[] count = new int[35];
                int i = 0;
                for(Page p:final_list){
                    if(i>=80) break;
                    i++;
                    //count.put(p.slab_no, count.getOrDefault(p.slab_no, 0)+1);
                    count[p.slab_no]++;
                }
                
//                File key_trace = new File("/Users/srinikhilreddy/Downloads/keytraces/"+s);
//                if(!key_trace.delete()){
 //                   System.out.println("Delete file failed");
   //             }
                String server = s.substring(0,s.indexOf("."));
                sendOverTelnet(server,count);
            }
            }
        }
    }
}


/**
 * 1. Knapsack approach where the bag is M memory pages and objects are items with its request rate being the profit of the item -- Not possible because
 *  then a page has to contain items from different slabs, which is not how memcached is modeled.
 * 
 * Counter Example. Consider 2 slab variants, where for slab1, you can fill 100 items per slab and for slab2 you can fill 1 item per slab.
 * Consider the request rate distribution of the items in the following manner.
 * 
 * 
 * -----------------------------------------> Decreasing request rate for items.
 * S1 | S2  | S1 | S1
 * 1  | 1   | 99 | 100
 *  -----------------------------------------> Decreasing request rate for items.

 * 
 * If we have only 2 pages, then by following the knapsack approach, we fill one page with 100 items of S1 and 1 item of S2.
 * But the optimal distribution would actually be 2 pages with 200 items of S1.
 * 
 * 
 */
