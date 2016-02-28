package operator;

import java.nio.ByteBuffer;

public class UsageExample
{
	
	public static void main(String[] args) throws Exception
	{
		String basePath = "C:/Users/yogi/workspace/FileBasedLogWriter/logFile";
		FileOperator fileOperator = FileOperator.createInstance(basePath);
		for (int i = 0; i < 50; i++)
		{
			String data = "Chhaya " + i;
			byte[] bytes = data.getBytes();
			ByteBuffer dataBuffer = ByteBuffer.wrap(bytes);
			fileOperator.write(dataBuffer);
		}
		System.out.println("All writtem");
		FileData readNext = null;
		while ((readNext = fileOperator.readNext()) != null)
		{
			if (readNext != null)
			{
				System.out.println(new String(readNext.getData()));
			}
			fileOperator.markAsRead(readNext.getOffset());
		}
	}
	
}
