package operator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class FileOperator
{
	private File										dataFile				= null;
	private File										indexFile				= null;
	private static final long							RESERVED_SIZE			= 8;
	private static final long							SINGLE_OFFSET_SIZE		= 8;
	private static final long							CONTENT_META_SIZE		= 4;
	
	private static final Map<String, RandomAccessFile>	CACHED_DATA_CHANNELS	= new ConcurrentHashMap<>();
	private static final Map<String, RandomAccessFile>	CACHED_INDEX_CHANNELS	= new ConcurrentHashMap<>();
	private static final Map<String, FileOperator>		CACHED_FILE_OPERATORS	= new ConcurrentHashMap<>();
	private static final ReentrantLock					LOCKER					= new ReentrantLock();
	
	private FileOperator(String path)
	{
		
		this.dataFile = new File(path);
		indexFile = new File(path + ".index");
	}
	
	/**
	 * It will write data to log file. First 4 bytes will be data length. Then
	 * actual data will start. Get last offset. Check it's data length. new msg
	 * pos = last offset + last data length
	 * 
	 * @throws IOException
	 * 
	 * */
	public synchronized void write(ByteBuffer buffer) throws IOException
	{
		long dataInsertOffset = getDataChannel().size();
		if (dataInsertOffset == 0)
		{
			// first write in file. No need to check for last position
			// first 8 bytes reserved
			dataInsertOffset = RESERVED_SIZE;
		}
		int currentDataLength = buffer.capacity();
		byte[] currentDLBytes = intToBytes(currentDataLength);
		FileChannel dataChannel = getDataChannel();
		MappedByteBuffer dataWriteBuffer = dataChannel.map(FileChannel.MapMode.READ_WRITE, dataInsertOffset, CONTENT_META_SIZE + currentDataLength);
		// currentDLBytes will be written first
		dataWriteBuffer.put(currentDLBytes);
		// Then actual data
		dataWriteBuffer.put(buffer);
	}
	
	/**
	 * It will read next data from log file that comes after last offset set
	 * using markAsRead(offset)
	 * 
	 * @throws IOException
	 * */
	public FileData readNext() throws IOException
	{
		FileChannel indexChannel = getIndexChannel();
		long size = indexChannel.size();
		long indexOffset = -1l;
		long currentDataOffset = -1;
		if (size != 0)
		{
			indexOffset = size - SINGLE_OFFSET_SIZE;
			ByteBuffer dataOffsetBuf = ByteBuffer.allocate((int) SINGLE_OFFSET_SIZE);
			indexChannel.read(dataOffsetBuf, indexOffset);
			dataOffsetBuf.flip();
			currentDataOffset = dataOffsetBuf.getLong();
			
			// That means there exists a last msgs which was marked read.
			// We got to skip it. Hence reading length meta.
			ByteBuffer lastDataLengthBuf = ByteBuffer.allocate((int) CONTENT_META_SIZE);
			FileChannel dataChannel = getDataChannel();
			dataChannel.read(lastDataLengthBuf, currentDataOffset);
			lastDataLengthBuf.flip();
			
			// We now know how long msg was
			int lastDataLength = lastDataLengthBuf.getInt();
			// We need to skip length meta length + data length
			currentDataOffset += CONTENT_META_SIZE + lastDataLength;
		}
		else
		{
			// nothing is present in index. So either no msg is present or none
			// is marked as read. That means we should point to first location
			// in data log. It may or may not be present
			currentDataOffset = RESERVED_SIZE;// since first 8 bytes are
												// reserved.
			
		}
		return read(currentDataOffset);
	}
	
	public synchronized void markAsRead(long offset) throws IOException
	{
		long dataInsertOffset = getIndexChannel().size();
		byte[] longToBytes = longToBytes(offset);
		FileChannel indexChannel = getIndexChannel();
		MappedByteBuffer indexBuffer = indexChannel.map(FileChannel.MapMode.READ_WRITE, dataInsertOffset, SINGLE_OFFSET_SIZE);
		indexBuffer.put(longToBytes);
	}
	
	/**
	 * Directly read data from log irrespective of it was marked as read or not
	 * based on offset
	 * 
	 * @throws IOException
	 */
	public FileData read(long offset) throws IOException
	{
		if (offset < RESERVED_SIZE)
		{
			// Invalid offset.
			return null;
		}
		FileChannel dataChannel = getDataChannel();
		ByteBuffer dataLengthBuf = ByteBuffer.allocate((int) CONTENT_META_SIZE);
		dataChannel.read(dataLengthBuf, offset);
		dataLengthBuf.flip();
		int dataLength = -1;
		try
		{
			dataLength = dataLengthBuf.getInt();
		}
		catch (BufferUnderflowException e)
		{
			// That means no data is present @ given offset
			return null;
		}
		long readFrom = offset + CONTENT_META_SIZE;
		ByteBuffer readData = ByteBuffer.allocate(dataLength);
		dataChannel.read(readData, readFrom);
		
		FileData data = new FileData();
		data.setBuffer(readData);
		data.setOffset(offset);
		return data;
	}
	
	static
	{
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				for (Entry<String, RandomAccessFile> entry : CACHED_DATA_CHANNELS.entrySet())
				{
					try
					{
						RandomAccessFile raf = entry.getValue();
						raf.getChannel().close();
						raf.close();
						System.out.println("Data file closed..");
					}
					catch (IOException e)
					{
						continue;
					}
				}
				for (Entry<String, RandomAccessFile> entry : CACHED_INDEX_CHANNELS.entrySet())
				{
					try
					{
						RandomAccessFile raf = entry.getValue();
						raf.getChannel().close();
						raf.close();
						System.out.println("index file closed..");
					}
					catch (IOException e)
					{
						continue;
					}
				}
			}
		}));
	}
	
	public static FileOperator createInstance(String path)
	{
		try
		{
			LOCKER.lock();
			FileOperator fw = CACHED_FILE_OPERATORS.get(path);
			if (fw == null)
			{
				fw = new FileOperator(path);
				CACHED_FILE_OPERATORS.put(path, fw);
			}
			return fw;
		}
		finally
		{
			LOCKER.unlock();
		}
	}
	
	private FileChannel getIndexChannel() throws FileNotFoundException
	{
		RandomAccessFile randomAccessFile = CACHED_INDEX_CHANNELS.get(indexFile.getAbsolutePath());
		if (randomAccessFile == null)
		{
			randomAccessFile = new RandomAccessFile(indexFile, "rw");
			CACHED_INDEX_CHANNELS.put(indexFile.getAbsolutePath(), randomAccessFile);
		}
		return randomAccessFile.getChannel();
	}
	
	private FileChannel getDataChannel() throws FileNotFoundException
	{
		RandomAccessFile randomAccessFile = CACHED_DATA_CHANNELS.get(dataFile.getAbsolutePath());
		if (randomAccessFile == null)
		{
			randomAccessFile = new RandomAccessFile(dataFile, "rw");
			CACHED_DATA_CHANNELS.put(dataFile.getAbsolutePath(), randomAccessFile);
		}
		return randomAccessFile.getChannel();
	}
	
	private byte[] intToBytes(int l)
	{
		byte[] result = new byte[4];
		for (int i = 3; i >= 0; i--)
		{
			result[i] = (byte) (l & 0xFF);
			l >>= 8;
		}
		return result;
	}
	
	private byte[] longToBytes(long l)
	{
		byte[] result = new byte[8];
		for (int i = 7; i >= 0; i--)
		{
			result[i] = (byte) (l & 0xFF);
			l >>= 8;
		}
		return result;
	}
	
}
