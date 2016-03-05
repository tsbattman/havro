
module Data.Avro.Generic (
    PrimitiveType(..)
  , NamedType(..)
  , ComplexType(..)
  , putAvroComplex
  , getAvroComplex
  , AvroType(..)
  , putAvro
  , getAvro
  , record
  ) where

import Control.Arrow (arr, (&&&), Kleisli(..))
import Data.Int

import Data.Binary
import Data.Binary.Put
import Data.Binary.Get
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LB
import qualified Data.Text as T
import qualified Data.Text.Lazy as LT
import qualified Data.Vector as V

import Data.Avro.Encoding
import Data.Avro.Schema

data PrimitiveType =
    AvroNull | AvroBool Bool | AvroInt Int32
  | AvroLong Int64 | AvroFloat Float | AvroDouble Double
  | AvroBytes LB.ByteString | AvroString LT.Text
  deriving (Eq, Show, Read)

putAvroPrimitive :: PrimitiveType -> Put
putAvroPrimitive AvroNull = put ()
putAvroPrimitive (AvroBool v) = put v
putAvroPrimitive (AvroInt v) = putAvroInt v
putAvroPrimitive (AvroLong v) = putAvroLong v
putAvroPrimitive (AvroFloat v) = putAvroFloat v
putAvroPrimitive (AvroDouble v) = putAvroDouble v
putAvroPrimitive (AvroBytes v) = putAvroBytes v
putAvroPrimitive (AvroString v) = putAvroString v

getAvroPrimitive :: PrimitiveSchemaType -> Get PrimitiveType
getAvroPrimitive NullSchema = return AvroNull
getAvroPrimitive BoolSchema = fmap AvroBool get
getAvroPrimitive IntSchema = fmap AvroInt getAvroInt
getAvroPrimitive LongSchema = fmap AvroLong getAvroLong
getAvroPrimitive FloatSchema = fmap AvroFloat getAvroFloat
getAvroPrimitive DoubleSchema = fmap AvroDouble getAvroDouble
getAvroPrimitive BytesSchema = fmap AvroBytes getAvroBytes
getAvroPrimitive StringSchema = fmap AvroString getAvroString

type AvroKV = (T.Text, AvroType)

putAvroKV :: AvroKV -> Put
putAvroKV (k, v) = putAvroString (LT.fromStrict k) >> putAvro v

getAvroKV :: TypeSchema -> Get AvroKV
getAvroKV s = (,) <$> fmap LT.toStrict getAvroString <*> getAvro s

putAvroBlock :: (a -> Put) -> V.Vector a -> Put
putAvroBlock f v = do
  putAvroLong (fromIntegral . V.length $ v)
  V.mapM_ f v

getAvroBlock :: Get a -> Get (V.Vector a)
getAvroBlock g = do
  l <- getAvroLong
  V.replicateM (fromIntegral l) g

putAvroBlocks :: (a -> Put) -> [V.Vector a] -> Put
putAvroBlocks f l = mapM_ (putAvroBlock f) l >> putWord8 0

getAvroBlocks :: Get a -> Get [V.Vector a]
getAvroBlocks g = do
  v <- getAvroBlock g
  if V.null v
    then return []
    else (v:) <$> getAvroBlocks g

data NamedType =
    AvroRecord [(String, AvroType)]
  | AvroEnum Int [String]
  | AvroFixed BS.ByteString
  deriving (Eq, Show, Read)

putAvroNamed :: NamedType -> Put
putAvroNamed (AvroRecord f) = mapM_ (putAvro . snd) f
putAvroNamed (AvroEnum e _) = putAvroInt (fromIntegral e)
putAvroNamed (AvroFixed f) = putByteString f

getAvroNamed :: NamedSchemaType -> Get NamedType
getAvroNamed (RecordSchema f) = AvroRecord <$> mapM (runKleisli $ arr fieldName &&& Kleisli (getAvro . toTypeSchema . fieldType)) f
getAvroNamed (EnumSchema f) = AvroEnum <$> fmap fromIntegral getAvroInt <*> pure f
getAvroNamed (FixedSchema n) = AvroFixed <$> getByteString n

data ComplexType =
    AvroNamed String (Maybe String) NamedType
  | AvroArray [V.Vector AvroType]
  | AvroMap [V.Vector AvroKV]
  | AvroUnion Int AvroType [Schema]
  deriving (Eq, Show, Read)

putAvroComplex :: ComplexType -> Put
putAvroComplex (AvroNamed _ _ r) = putAvroNamed r
putAvroComplex (AvroArray b) = putAvroBlocks putAvro b
putAvroComplex (AvroMap b) = putAvroBlocks putAvroKV b
putAvroComplex (AvroUnion ix v _) = putAvroInt (fromIntegral ix) >> putAvro v

getAvroComplex :: ComplexSchemaType -> Get ComplexType
getAvroComplex (NamedSchema nm ns _ r) = AvroNamed nm ns <$> getAvroNamed r
getAvroComplex (ArraySchema s) = AvroArray <$> getAvroBlocks (getAvro (toTypeSchema s))
getAvroComplex (MapSchema s) = AvroMap <$> getAvroBlocks (getAvroKV (toTypeSchema s))
getAvroComplex (UnionSchema s) = do
  ix <- fromIntegral <$> getAvroInt
  AvroUnion ix <$> getAvro (toTypeSchema $ s !! ix) <*> pure s

data AvroType =
    AvroPrimitive PrimitiveType
  | AvroComplex ComplexType
  deriving (Eq, Show, Read)

putAvro :: AvroType -> Put
putAvro (AvroPrimitive v) = putAvroPrimitive v
putAvro (AvroComplex v) = putAvroComplex v

getAvro :: TypeSchema -> Get AvroType
getAvro (PrimitiveSchema s) = AvroPrimitive <$> getAvroPrimitive s
getAvro (ComplexSchema s) = AvroComplex <$> getAvroComplex s

record :: String -> Maybe String -> [(String, AvroType)] -> AvroType
record nm ns = AvroComplex . AvroNamed nm ns . AvroRecord
