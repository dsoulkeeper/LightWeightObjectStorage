package operator;

import java.nio.ByteBuffer;

public class FileData
{
	private byte[]		data	= null;
	private ByteBuffer	buffer	= null;
	private long		offset	= -1;
	
	public long getOffset()
	{
		return offset;
	}
	
	public void setOffset(long offset)
	{
		this.offset = offset;
	}
	
	public ByteBuffer getBuffer()
	{
		if (buffer == null)
			buffer = ByteBuffer.wrap(data);
		return buffer;
	}
	
	public void setBuffer(ByteBuffer buffer)
	{
		this.buffer = buffer;
	}
	
	public byte[] getData()
	{
		if (data == null)
			data = buffer.array();
		return data;
	}
	
	public void setData(byte[] data)
	{
		this.data = data;
	}
}
