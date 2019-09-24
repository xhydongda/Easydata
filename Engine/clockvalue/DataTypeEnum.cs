namespace Easydata.Engine
{
    /// <summary>
    /// 数据类型，决定了编码与解码方法.
    /// </summary>
    public class DataTypeEnum
    {
        /// <summary>
        /// 浮点型.
        /// </summary>
        public const byte Double = 0x00;

        /// <summary>
        /// 整型.
        /// </summary>
        public const byte Integer = 0x01;

        /// <summary>
        /// 布尔型.
        /// </summary>
        public const byte Boolean = 0x02;

        /// <summary>
        /// 字符串型.
        /// </summary>
	    public const byte String = 0x03;

        /// <summary>
        /// 时标型.
        /// </summary>
        public const byte DateTime = 0x04;

        /// <summary>
        /// 空类型，长度为0.
        /// </summary>
        public const byte Empty = 0x05;
    }
}
