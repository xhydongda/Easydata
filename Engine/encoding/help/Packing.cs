using System;

namespace Easydata.Engine
{
    public delegate ulong packmethod(Span<ulong> src);
    public delegate void unpackmethod(ulong v, Span<ulong> dst);
    public class Packing
    {
        public static Packing[] All = new Packing[] {
            new Packing(240,0, new packmethod(pack240),new unpackmethod(unpack240)),
            new Packing(120,0, new packmethod(pack120),new unpackmethod(unpack120)),
            new Packing(60,1, new packmethod(pack60),new unpackmethod(unpack60)),
            new Packing(30,2, new packmethod(pack30),new unpackmethod(unpack30)),
            new Packing(20,3, new packmethod(pack20),new unpackmethod(unpack20)),
            new Packing(15,4, new packmethod(pack15),new unpackmethod(unpack15)),
            new Packing(12,5, new packmethod(pack12),new unpackmethod(unpack12)),
            new Packing(10,6, new packmethod(pack10),new unpackmethod(unpack10)),
            new Packing(8,7, new packmethod(pack8),new unpackmethod(unpack8)),
            new Packing(7,8, new packmethod(pack7),new unpackmethod(unpack7)),
            new Packing(6,10, new packmethod(pack6),new unpackmethod(unpack6)),
            new Packing(5,12, new packmethod(pack5),new unpackmethod(unpack5)),
            new Packing(4,15, new packmethod(pack4),new unpackmethod(unpack4)),
            new Packing(3,20, new packmethod(pack3),new unpackmethod(unpack3)),
            new Packing(2,30, new packmethod(pack2),new unpackmethod(unpack2)),
            new Packing(1,60, new packmethod(pack1),new unpackmethod(unpack1))
        };

        readonly int _bit;
        packmethod _pack;
        unpackmethod _unpack;
        public Packing(int n, int bit, packmethod pack, unpackmethod unpack)
        {
            this.n = n;
            _bit = bit;
            _pack = pack;
            _unpack = unpack;
        }

        public int n { get; }

        public ulong pack(int startIndex, ulong[] src)
        {
            Span<ulong> span = new Span<ulong>(src).Slice(startIndex);
            return _pack.Invoke(span);
        }

        public ulong pack(Span<ulong> src)
        {
            return _pack.Invoke(src);
        }

        public void unpack(ulong v, ulong[] dst)
        {
            _unpack.Invoke(v, new Span<ulong>(dst));
        }
        public void unpack(ulong v, Span<ulong> dst)
        {
            _unpack.Invoke(v, dst);
        }

        #region pack

        // pack240 packs 240 ones from in using 1 bit each
        public static ulong pack240(Span<ulong> src)
        {
            return 0;
        }

        // pack120 packs 120 ones from in using 1 bit each
        public static ulong pack120(Span<ulong> src)
        {
            return 0;
        }

        const ulong u2_60 = (ulong)2 << 60;
        // pack60 packs 60 values from in using 1 bit each
        public static ulong pack60(Span<ulong> src)
        {
            return u2_60 |
                src[0] |
                src[1] << 1 |
                src[2] << 2 |
                src[3] << 3 |
                src[4] << 4 |
                src[5] << 5 |
                src[6] << 6 |
                src[7] << 7 |
                src[8] << 8 |
                src[9] << 9 |
                src[10] << 10 |
                src[11] << 11 |
                src[12] << 12 |
                src[13] << 13 |
                src[14] << 14 |
                src[15] << 15 |
                src[16] << 16 |
                src[17] << 17 |
                src[18] << 18 |
                src[19] << 19 |
                src[20] << 20 |
                src[21] << 21 |
                src[22] << 22 |
                src[23] << 23 |
                src[24] << 24 |
                src[25] << 25 |
                src[26] << 26 |
                src[27] << 27 |
                src[28] << 28 |
                src[29] << 29 |
                src[30] << 30 |
                src[31] << 31 |
                src[32] << 32 |
                src[33] << 33 |
                src[34] << 34 |
                src[35] << 35 |
                src[36] << 36 |
                src[37] << 37 |
                src[38] << 38 |
                src[39] << 39 |
                src[40] << 40 |
                src[41] << 41 |
                src[42] << 42 |
                src[43] << 43 |
                src[44] << 44 |
                src[45] << 45 |
                src[46] << 46 |
                src[47] << 47 |
                src[48] << 48 |
                src[49] << 49 |
                src[50] << 50 |
                src[51] << 51 |
                src[52] << 52 |
                src[53] << 53 |
                src[54] << 54 |
                src[55] << 55 |
                src[56] << 56 |
                src[57] << 57 |
                src[58] << 58 |
                src[59] << 59;

        }

        const ulong u3_60 = (ulong)3 << 60;
        // pack30 packs 30 values from in using 2 bits each
        public static ulong pack30(Span<ulong> src)
        {
            return u3_60 |
                src[0] |
                src[1] << 2 |
                src[2] << 4 |
                src[3] << 6 |
                src[4] << 8 |
                src[5] << 10 |
                src[6] << 12 |
                src[7] << 14 |
                src[8] << 16 |
                src[9] << 18 |
                src[10] << 20 |
                src[11] << 22 |
                src[12] << 24 |
                src[13] << 26 |
                src[14] << 28 |
                src[15] << 30 |
                src[16] << 32 |
                src[17] << 34 |
                src[18] << 36 |
                src[19] << 38 |
                src[20] << 40 |
                src[21] << 42 |
                src[22] << 44 |
                src[23] << 46 |
                src[24] << 48 |
                src[25] << 50 |
                src[26] << 52 |
                src[27] << 54 |
                src[28] << 56 |
                src[29] << 58;
        }

        const ulong u4_60 = (ulong)4 << 60;
        // pack20 packs 20 values from in using 3 bits each
        public static ulong pack20(Span<ulong> src)
        {
            return u4_60 |
                src[0] |
                src[1] << 3 |
                src[2] << 6 |
                src[3] << 9 |
                src[4] << 12 |
                src[5] << 15 |
                src[6] << 18 |
                src[7] << 21 |
                src[8] << 24 |
                src[9] << 27 |
                src[10] << 30 |
                src[11] << 33 |
                src[12] << 36 |
                src[13] << 39 |
                src[14] << 42 |
                src[15] << 45 |
                src[16] << 48 |
                src[17] << 51 |
                src[18] << 54 |
                src[19] << 57;
        }

        const ulong u5_60 = (ulong)5 << 60;
        // pack15 packs 15 values from in using 3 bits each
        public static ulong pack15(Span<ulong> src)
        {
            return u5_60 |
                src[0] |
                src[1] << 4 |
                src[2] << 8 |
                src[3] << 12 |
                src[4] << 16 |
                src[5] << 20 |
                src[6] << 24 |
                src[7] << 28 |
                src[8] << 32 |
                src[9] << 36 |
                src[10] << 40 |
                src[11] << 44 |
                src[12] << 48 |
                src[13] << 52 |
                src[14] << 56;
        }

        const ulong u6_60 = (ulong)6 << 60;
        // pack12 packs 12 values from in using 5 bits each
        public static ulong pack12(Span<ulong> src)
        {
            return u6_60 |
                src[0] |
                src[1] << 5 |
                src[2] << 10 |
                src[3] << 15 |
                src[4] << 20 |
                src[5] << 25 |
                src[6] << 30 |
                src[7] << 35 |
                src[8] << 40 |
                src[9] << 45 |
                src[10] << 50 |
                src[11] << 55;
        }

        const ulong u7_60 = (ulong)7 << 60;
        // pack10 packs 10 values from in using 6 bits each
        public static ulong pack10(Span<ulong> src)
        {
            return u7_60 |
                src[0] |
                src[1] << 6 |
                src[2] << 12 |
                src[3] << 18 |
                src[4] << 24 |
                src[5] << 30 |
                src[6] << 36 |
                src[7] << 42 |
                src[8] << 48 |
                src[9] << 54;
        }

        const ulong u8_60 = (ulong)8 << 60;
        // pack8 packs 8 values from in using 7 bits each
        public static ulong pack8(Span<ulong> src)
        {
            return u8_60 |
                src[0] |
                src[1] << 7 |
                src[2] << 14 |
                src[3] << 21 |
                src[4] << 28 |
                src[5] << 35 |
                src[6] << 42 |
                src[7] << 49;
        }

        const ulong u9_60 = (ulong)9 << 60;
        // pack7 packs 7 values from in using 8 bits each
        public static ulong pack7(Span<ulong> src)
        {
            return u9_60 |
                src[0] |
                src[1] << 8 |
                src[2] << 16 |
                src[3] << 24 |
                src[4] << 32 |
                src[5] << 40 |
                src[6] << 48;
        }

        const ulong u10_60 = (ulong)10 << 60;
        // pack6 packs 6 values from in using 10 bits each
        public static ulong pack6(Span<ulong> src)
        {
            return u10_60 |
                src[0] |
                src[1] << 10 |
                src[2] << 20 |
                src[3] << 30 |
                src[4] << 40 |
                src[5] << 50;
        }

        const ulong u11_60 = (ulong)11 << 60;
        // pack5 packs 5 values from in using 12 bits each
        public static ulong pack5(Span<ulong> src)
        {
            return u11_60 |
                src[0] |
                src[1] << 12 |
                src[2] << 24 |
                src[3] << 36 |
                src[4] << 48;
        }

        const ulong u12_60 = (ulong)12 << 60;
        // pack4 packs 4 values from in using 15 bits each
        public static ulong pack4(Span<ulong> src)
        {
            return u12_60 |
                src[0] |
                src[1] << 15 |
                src[2] << 30 |
                src[3] << 45;
        }

        const ulong u13_60 = (ulong)13 << 60;
        // pack3 packs 3 values from in using 20 bits each
        public static ulong pack3(Span<ulong> src)
        {
            return u13_60 |
                src[0] |
                src[1] << 20 |
                src[2] << 40;
        }

        const ulong u14_60 = (ulong)14 << 60;
        // pack2 packs 2 values from in using 30 bits each
        public static ulong pack2(Span<ulong> src)
        {
            return u14_60 |
                src[0] |
                src[1] << 30;
        }

        const ulong u15_60 = (ulong)15 << 60;
        // pack1 packs 1 values from in using 60 bits each
        public static ulong pack1(Span<ulong> src)
        {
            return u15_60 |
                src[0];
        }

        #endregion


        #region unpack

        private static void unpack240(ulong v, Span<ulong> dst)
        {
            for (int i = 0; i < dst.Length; i++)
            {
                dst[i] = 1;
            }
        }

        private static void unpack120(ulong v, Span<ulong> dst)
        {
            for (int i = 0; i < dst.Length; i++)
            {
                dst[i] = 1;
            }
        }

        private static void unpack60(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 1;
            dst[1] = (v >> 1) & 1;
            dst[2] = (v >> 2) & 1;
            dst[3] = (v >> 3) & 1;
            dst[4] = (v >> 4) & 1;
            dst[5] = (v >> 5) & 1;
            dst[6] = (v >> 6) & 1;
            dst[7] = (v >> 7) & 1;
            dst[8] = (v >> 8) & 1;
            dst[9] = (v >> 9) & 1;
            dst[10] = (v >> 10) & 1;
            dst[11] = (v >> 11) & 1;
            dst[12] = (v >> 12) & 1;
            dst[13] = (v >> 13) & 1;
            dst[14] = (v >> 14) & 1;
            dst[15] = (v >> 15) & 1;
            dst[16] = (v >> 16) & 1;
            dst[17] = (v >> 17) & 1;
            dst[18] = (v >> 18) & 1;
            dst[19] = (v >> 19) & 1;
            dst[20] = (v >> 20) & 1;
            dst[21] = (v >> 21) & 1;
            dst[22] = (v >> 22) & 1;
            dst[23] = (v >> 23) & 1;
            dst[24] = (v >> 24) & 1;
            dst[25] = (v >> 25) & 1;
            dst[26] = (v >> 26) & 1;
            dst[27] = (v >> 27) & 1;
            dst[28] = (v >> 28) & 1;
            dst[29] = (v >> 29) & 1;
            dst[30] = (v >> 30) & 1;
            dst[31] = (v >> 31) & 1;
            dst[32] = (v >> 32) & 1;
            dst[33] = (v >> 33) & 1;
            dst[34] = (v >> 34) & 1;
            dst[35] = (v >> 35) & 1;
            dst[36] = (v >> 36) & 1;
            dst[37] = (v >> 37) & 1;
            dst[38] = (v >> 38) & 1;
            dst[39] = (v >> 39) & 1;
            dst[40] = (v >> 40) & 1;
            dst[41] = (v >> 41) & 1;
            dst[42] = (v >> 42) & 1;
            dst[43] = (v >> 43) & 1;
            dst[44] = (v >> 44) & 1;
            dst[45] = (v >> 45) & 1;
            dst[46] = (v >> 46) & 1;
            dst[47] = (v >> 47) & 1;
            dst[48] = (v >> 48) & 1;
            dst[49] = (v >> 49) & 1;
            dst[50] = (v >> 50) & 1;
            dst[51] = (v >> 51) & 1;
            dst[52] = (v >> 52) & 1;
            dst[53] = (v >> 53) & 1;
            dst[54] = (v >> 54) & 1;
            dst[55] = (v >> 55) & 1;
            dst[56] = (v >> 56) & 1;
            dst[57] = (v >> 57) & 1;
            dst[58] = (v >> 58) & 1;
            dst[59] = (v >> 59) & 1;
        }

        private static void unpack30(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 3;
            dst[1] = (v >> 2) & 3;
            dst[2] = (v >> 4) & 3;
            dst[3] = (v >> 6) & 3;
            dst[4] = (v >> 8) & 3;
            dst[5] = (v >> 10) & 3;
            dst[6] = (v >> 12) & 3;
            dst[7] = (v >> 14) & 3;
            dst[8] = (v >> 16) & 3;
            dst[9] = (v >> 18) & 3;
            dst[10] = (v >> 20) & 3;
            dst[11] = (v >> 22) & 3;
            dst[12] = (v >> 24) & 3;
            dst[13] = (v >> 26) & 3;
            dst[14] = (v >> 28) & 3;
            dst[15] = (v >> 30) & 3;
            dst[16] = (v >> 32) & 3;
            dst[17] = (v >> 34) & 3;
            dst[18] = (v >> 36) & 3;
            dst[19] = (v >> 38) & 3;
            dst[20] = (v >> 40) & 3;
            dst[21] = (v >> 42) & 3;
            dst[22] = (v >> 44) & 3;
            dst[23] = (v >> 46) & 3;
            dst[24] = (v >> 48) & 3;
            dst[25] = (v >> 50) & 3;
            dst[26] = (v >> 52) & 3;
            dst[27] = (v >> 54) & 3;
            dst[28] = (v >> 56) & 3;
            dst[29] = (v >> 58) & 3;
        }

        private static void unpack20(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 7;
            dst[1] = (v >> 3) & 7;
            dst[2] = (v >> 6) & 7;
            dst[3] = (v >> 9) & 7;
            dst[4] = (v >> 12) & 7;
            dst[5] = (v >> 15) & 7;
            dst[6] = (v >> 18) & 7;
            dst[7] = (v >> 21) & 7;
            dst[8] = (v >> 24) & 7;
            dst[9] = (v >> 27) & 7;
            dst[10] = (v >> 30) & 7;
            dst[11] = (v >> 33) & 7;
            dst[12] = (v >> 36) & 7;
            dst[13] = (v >> 39) & 7;
            dst[14] = (v >> 42) & 7;
            dst[15] = (v >> 45) & 7;
            dst[16] = (v >> 48) & 7;
            dst[17] = (v >> 51) & 7;
            dst[18] = (v >> 54) & 7;
            dst[19] = (v >> 57) & 7;
        }

        private static void unpack15(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 15;
            dst[1] = (v >> 4) & 15;
            dst[2] = (v >> 8) & 15;
            dst[3] = (v >> 12) & 15;
            dst[4] = (v >> 16) & 15;
            dst[5] = (v >> 20) & 15;
            dst[6] = (v >> 24) & 15;
            dst[7] = (v >> 28) & 15;
            dst[8] = (v >> 32) & 15;
            dst[9] = (v >> 36) & 15;
            dst[10] = (v >> 40) & 15;
            dst[11] = (v >> 44) & 15;
            dst[12] = (v >> 48) & 15;
            dst[13] = (v >> 52) & 15;
            dst[14] = (v >> 56) & 15;
        }

        private static void unpack12(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 31;
            dst[1] = (v >> 5) & 31;
            dst[2] = (v >> 10) & 31;
            dst[3] = (v >> 15) & 31;
            dst[4] = (v >> 20) & 31;
            dst[5] = (v >> 25) & 31;
            dst[6] = (v >> 30) & 31;
            dst[7] = (v >> 35) & 31;
            dst[8] = (v >> 40) & 31;
            dst[9] = (v >> 45) & 31;
            dst[10] = (v >> 50) & 31;
            dst[11] = (v >> 55) & 31;
        }

        private static void unpack10(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 63;
            dst[1] = (v >> 6) & 63;
            dst[2] = (v >> 12) & 63;
            dst[3] = (v >> 18) & 63;
            dst[4] = (v >> 24) & 63;
            dst[5] = (v >> 30) & 63;
            dst[6] = (v >> 36) & 63;
            dst[7] = (v >> 42) & 63;
            dst[8] = (v >> 48) & 63;
            dst[9] = (v >> 54) & 63;
        }

        private static void unpack8(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 127;
            dst[1] = (v >> 7) & 127;
            dst[2] = (v >> 14) & 127;
            dst[3] = (v >> 21) & 127;
            dst[4] = (v >> 28) & 127;
            dst[5] = (v >> 35) & 127;
            dst[6] = (v >> 42) & 127;
            dst[7] = (v >> 49) & 127;
        }

        private static void unpack7(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 255;
            dst[1] = (v >> 8) & 255;
            dst[2] = (v >> 16) & 255;
            dst[3] = (v >> 24) & 255;
            dst[4] = (v >> 32) & 255;
            dst[5] = (v >> 40) & 255;
            dst[6] = (v >> 48) & 255;
        }

        private static void unpack6(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 1023;
            dst[1] = (v >> 10) & 1023;
            dst[2] = (v >> 20) & 1023;
            dst[3] = (v >> 30) & 1023;
            dst[4] = (v >> 40) & 1023;
            dst[5] = (v >> 50) & 1023;
        }

        private static void unpack5(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 4095;
            dst[1] = (v >> 12) & 4095;
            dst[2] = (v >> 24) & 4095;
            dst[3] = (v >> 36) & 4095;
            dst[4] = (v >> 48) & 4095;
        }

        private static void unpack4(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 32767;
            dst[1] = (v >> 15) & 32767;
            dst[2] = (v >> 30) & 32767;
            dst[3] = (v >> 45) & 32767;
        }

        private static void unpack3(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 1048575;
            dst[1] = (v >> 20) & 1048575;
            dst[2] = (v >> 40) & 1048575;
        }

        private static void unpack2(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 1073741823;
            dst[1] = (v >> 30) & 1073741823;
        }

        private static void unpack1(ulong v, Span<ulong> dst)
        {
            dst[0] = v & 1152921504606846975;
        }
        #endregion
    }
}
