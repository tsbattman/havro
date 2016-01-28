
module Data.Avro.Encoding (
    encodeZigZag
  , decodeZigZag
  , putVarInt
  , getVarInt
  , floatToIntBits
  , intBitsToFloat
  , doubleToLongBits
  , longBitsToDouble
  , putAvroInt
  , getAvroInt
  , putAvroLong
  , getAvroLong
  , putAvroFloat
  , getAvroFloat
  , putAvroDouble
  , getAvroDouble
  , putAvroBytes
  , getAvroBytes
  , putAvroString
  , getAvroString
  , putAvroEnum
  , getAvroEnum
  ) where

import Control.Monad (replicateM)
import Data.Int
import Data.Word
import Data.Bits
import System.IO.Unsafe
import Foreign
import qualified Data.ByteString.Lazy as LB
import qualified Data.Text.Lazy as LT
import qualified Data.Text.Lazy.Encoding as LT
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put

shiftLogicalR :: FiniteBits a => a -> Int -> a
shiftLogicalR a n = (a `shiftR` n) .&. complement mask
  where mask = setBit zeroBits (finiteBitSize a - 1) `shiftR` (n - 1)

encodeZigZag :: FiniteBits a => a -> a
encodeZigZag a = (a `shiftL` 1) `xor` (a `shiftR` (finiteBitSize a - 1))

decodeZigZag :: (Num a, FiniteBits a) => a -> a
decodeZigZag a = (a `shiftLogicalR` 1) `xor` negate (a .&. 1)

putVarInt :: (Integral a, FiniteBits a) => a -> Put
putVarInt a | a .&. complement 0x7F == zeroBits = putWord8 $ fromIntegral a
putVarInt a = putWord8 (fromIntegral $ (a .&. 0x7F) .|. 0x80) >> putVarInt (a `shiftLogicalR` 7)

getVarInt :: (Integral a, FiniteBits a) => Get a
getVarInt = do
  x <- getWord8
  if x `testBit` 7
    then fmap (\g -> g `shiftL` 7 .|. fromIntegral (x .&. 0x7f)) getVarInt
    else return $ fromIntegral x

floatToIntBits :: Float -> Word32
floatToIntBits x
  | isInfinite x && x > 0 = 0x7F800000
  | isInfinite x && x < 0 = 0xFF800000
  | isNaN x = 0x7FC00000
  | otherwise = unsafePerformIO . alloca $ \p -> poke p x >> peek (castPtr p)

intBitsToFloat :: Word32 -> Float
intBitsToFloat x = unsafePerformIO . alloca $ \p -> poke p x >> peek (castPtr p)

doubleToLongBits :: Double -> Word64
doubleToLongBits x
  | isInfinite x && x > 0 = 0x7FF0000000000000
  | isInfinite x && x < 0 = 0xFFF0000000000000
  | isNaN x = 0x7FF8000000000000
  | otherwise = unsafePerformIO . alloca $ \p -> poke p x >> peek (castPtr p)

longBitsToDouble :: Word64 -> Double
longBitsToDouble x = unsafePerformIO . alloca $ \p -> poke p x >> peek (castPtr p)

putAvroInt :: Int32 -> Put
putAvroInt = putVarInt . encodeZigZag

getAvroInt :: Get Int32
getAvroInt = fmap decodeZigZag getVarInt

putAvroLong :: Int64 -> Put
putAvroLong = putVarInt . encodeZigZag

getAvroLong :: Get Int64
getAvroLong = fmap decodeZigZag getVarInt

putAvroFloat :: Float -> Put
putAvroFloat = put . floatToIntBits

getAvroFloat :: Get Float
getAvroFloat = fmap intBitsToFloat get

putAvroDouble  :: Double -> Put
putAvroDouble = put . doubleToLongBits

getAvroDouble :: Get Double
getAvroDouble = fmap longBitsToDouble get

putAvroBytes :: LB.ByteString -> Put
putAvroBytes b = putAvroLong (LB.length b) >> putLazyByteString b

getAvroBytes :: Get LB.ByteString
getAvroBytes = getAvroLong >>= getLazyByteString

putAvroString :: LT.Text -> Put
putAvroString = putAvroBytes . LT.encodeUtf8

getAvroString :: Get LT.Text
getAvroString = fmap LT.decodeUtf8 getAvroBytes

putAvroEnum :: Enum e => e -> Put
putAvroEnum = putAvroInt . fromIntegral . fromEnum

getAvroEnum :: Enum e => Get e
getAvroEnum = fmap (toEnum . fromIntegral) getAvroInt
