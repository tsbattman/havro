{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE TupleSections #-}

module Data.Avro.Class (
    ToAvro(..)
  , FromAvro(..)
  ) where

import Data.Bifunctor
import Data.Int
import Data.Word

import Data.Binary.Put
import Data.Binary.Get
import qualified Data.Map as Map
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LB
import qualified Data.Text as T
import qualified Data.Text.Lazy as LT
import qualified Data.Vector as V

import Data.Avro.Schema
import Data.Avro.Generic

class ToAvro a where
  avroSchema :: a -> Schema
  toAvro :: a -> AvroType
  toBinary :: a -> Put
  toBinary = putAvro . toAvro

instance ToAvro () where
  avroSchema () = plainSchema $ PrimitiveSchema NullSchema
  toAvro () = AvroPrimitive AvroNull

instance ToAvro Bool where
  avroSchema _ = plainSchema $ PrimitiveSchema BoolSchema
  toAvro = AvroPrimitive . AvroBool

instance ToAvro Int32 where
  avroSchema _ = plainSchema $ PrimitiveSchema IntSchema
  toAvro = AvroPrimitive . AvroInt

instance ToAvro Int64 where
  avroSchema _ = plainSchema $ PrimitiveSchema LongSchema
  toAvro = AvroPrimitive . AvroLong

instance ToAvro Int where
  avroSchema _ = plainSchema $ PrimitiveSchema LongSchema
  toAvro = AvroPrimitive . AvroLong . fromIntegral

instance ToAvro Float where
  avroSchema _ = plainSchema $ PrimitiveSchema FloatSchema
  toAvro = AvroPrimitive . AvroFloat

instance ToAvro Double where
  avroSchema _ = plainSchema $ PrimitiveSchema DoubleSchema
  toAvro = AvroPrimitive . AvroDouble

instance ToAvro BS.ByteString where
  avroSchema _ = plainSchema $ PrimitiveSchema BytesSchema
  toAvro = AvroPrimitive . AvroBytes . LB.fromStrict

instance ToAvro LB.ByteString where
  avroSchema _ = plainSchema $ PrimitiveSchema BytesSchema
  toAvro = AvroPrimitive . AvroBytes

instance ToAvro [Word8] where
  avroSchema _ = plainSchema $ PrimitiveSchema BytesSchema
  toAvro = AvroPrimitive . AvroBytes . LB.pack

instance ToAvro T.Text where
  avroSchema _ = plainSchema $ PrimitiveSchema StringSchema
  toAvro = AvroPrimitive . AvroString . LT.fromStrict

instance ToAvro LT.Text where
  avroSchema _ = plainSchema $ PrimitiveSchema StringSchema
  toAvro = AvroPrimitive . AvroString

instance ToAvro String where
  avroSchema _ = plainSchema $ PrimitiveSchema StringSchema
  toAvro = AvroPrimitive . AvroString . LT.pack

instance ToAvro PrimitiveType where
  avroSchema AvroNull = plainSchema $ PrimitiveSchema NullSchema
  avroSchema (AvroBool _) = plainSchema $ PrimitiveSchema BoolSchema
  avroSchema (AvroInt _) = plainSchema $ PrimitiveSchema IntSchema
  avroSchema (AvroLong _) = plainSchema $ PrimitiveSchema LongSchema
  avroSchema (AvroFloat _) = plainSchema $ PrimitiveSchema FloatSchema
  avroSchema (AvroDouble _) = plainSchema $ PrimitiveSchema DoubleSchema
  avroSchema (AvroBytes _) = plainSchema $ PrimitiveSchema BytesSchema
  avroSchema (AvroString _) = plainSchema $ PrimitiveSchema StringSchema
  toAvro = AvroPrimitive

instance ToAvro ComplexType where
  avroSchema (AvroRecord f) = plainSchema . ComplexSchema $ recordSchema "" Nothing [] (map (recordField "" . toTypeSchema . avroSchema) f)
  avroSchema (AvroEnum _ f) = plainSchema . ComplexSchema $ enumSchema "" Nothing [] f
  avroSchema (AvroFixed f) = plainSchema . ComplexSchema $ fixedSchema "" Nothing [] (BS.length f)
  avroSchema (AvroArray f) = plainSchema . ComplexSchema $ ArraySchema (toTypeSchema . avroSchema . V.head . head $ f)
  avroSchema (AvroMap f) = plainSchema . ComplexSchema $ MapSchema (toTypeSchema . avroSchema . snd . V.head . head $ f)
  avroSchema (AvroUnion _ _ f) = plainSchema . ComplexSchema $ UnionSchema f
  toAvro = AvroComplex

instance ToAvro AvroType where
  avroSchema (AvroPrimitive f) = avroSchema f
  avroSchema (AvroComplex f) = avroSchema f
  toAvro = id

class FromAvro a where
  fromAvro :: Monad m => Schema -> AvroType -> m a
  fromBinary :: Schema -> Get a
  fromBinary s = getAvro (toTypeSchema s) >>= fromAvro s

instance FromAvro () where
  fromAvro _ (AvroPrimitive AvroNull) = return ()
  fromAvro _ r = fail $ "expected () got " ++ show r

instance FromAvro Bool where
  fromAvro _ (AvroPrimitive (AvroBool v)) = return v
  fromAvro _ r = fail $ "expected bool got " ++ show r

instance FromAvro Int32 where
  fromAvro _ (AvroPrimitive (AvroInt v)) = return v
  fromAvro _ r = fail $ "expected int got " ++ show r

instance FromAvro Int64 where
  fromAvro _ (AvroPrimitive (AvroLong v)) = return v
  fromAvro _ r = fail $ "expected long got " ++ show r

instance FromAvro Int where
  fromAvro _ (AvroPrimitive (AvroLong v)) = return (fromIntegral v)
  fromAvro _ (AvroPrimitive (AvroInt v)) = return (fromIntegral v)
  fromAvro _ r = fail $ "expected int/long got " ++ show r

instance FromAvro Float where
  fromAvro _ (AvroPrimitive (AvroFloat v)) = return v
  fromAvro _ r = fail $ "expected float got " ++ show r

instance FromAvro Double where
  fromAvro _ (AvroPrimitive (AvroDouble v)) = return v
  fromAvro _ r = fail $ "expected double got " ++ show r

instance FromAvro BS.ByteString where
  fromAvro _ (AvroComplex (AvroFixed v)) = return v
  fromAvro _ (AvroPrimitive (AvroBytes v)) = return (LB.toStrict v)
  fromAvro _ r = fail $ "expected bytes got " ++ show r

instance FromAvro LB.ByteString where
  fromAvro _ (AvroComplex (AvroFixed v)) = return (LB.fromStrict v)
  fromAvro _ (AvroPrimitive (AvroBytes v)) = return v
  fromAvro _ r = fail $ "expected bytes got " ++ show r

instance FromAvro [Word8] where
  fromAvro s = fmap BS.unpack . fromAvro s

instance FromAvro T.Text where
  fromAvro _ (AvroPrimitive (AvroString v)) = return (LT.toStrict v)
  fromAvro _ r = fail $ "expected string got " ++ show r

instance FromAvro LT.Text where
  fromAvro _ (AvroPrimitive (AvroString v)) = return v
  fromAvro _ r = fail $ "expected string got " ++ show r

instance FromAvro String where
  fromAvro s = fmap T.unpack . fromAvro s

instance FromAvro PrimitiveType where
  fromAvro _ (AvroPrimitive v) = return v
  fromAvro _ r = fail $ "expecting primitive got " ++ show r

instance FromAvro ComplexType where
  fromAvro _ (AvroComplex v) = return v
  fromAvro _ r = fail $ "expecting complex got " ++ show r

instance FromAvro AvroType where
  fromAvro _ = return

instance ToAvro a => ToAvro (Map.Map T.Text a) where
  avroSchema _ = avroSchema $ AvroMap (undefined :: [V.Vector a])
  toAvro = toAvro . AvroMap . return . V.fromList . map (second toAvro) . Map.toList

instance FromAvro a => FromAvro (Map.Map T.Text a) where
  fromAvro s@(toTypeSchema -> ComplexSchema (MapSchema ms)) v = do
    AvroComplex (AvroMap r) <- fromAvro s v
    fmap Map.fromList . mapM go . concatMap V.toList $ r
    where go (x, y) = (x,) <$> fromAvro (plainSchema ms) y
