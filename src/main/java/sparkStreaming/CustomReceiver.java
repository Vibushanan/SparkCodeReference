package sparkStreaming;

import java.io.File;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/*
 * This is a sample custom receiver just list the files on a given directory
 */

public class CustomReceiver extends Receiver<String>{

	String directory;
	String list_Files="";
	
//Constructor - what u pass through your code	
	public CustomReceiver(String _directory) {
		super(StorageLevel.MEMORY_AND_DISK());
		directory=_directory;
	}

	
	//On start - call the processing module receive();
	@Override
	public void onStart() {
		
		new Thread(){
			
			public void run(){
				receive();
			}
			
		}.start();
		
		
	}

	@Override
	public void onStop() {
		
	}

	
	//receive() - has the logic to perform

	private void receive(){
		try{
		File file = new File(directory);
		
		System.out.println("Connected Directory :"+directory);
		
		File[] listOfFiles = file.listFiles();
		
		list_Files="";
		for(File _file:listOfFiles){
			list_Files+=_file.getName();
		}
		
		store(list_Files);
		
		
		
		}catch(Throwable t){
			 restart("Error receiving data", t);
		}
	}
	
	
}
