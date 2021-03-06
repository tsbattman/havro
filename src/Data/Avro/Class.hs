{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TupleSections #-}

module Data.Avro.Class (
    ToAvro(..)
  , FromAvro(..)
  , flookup
  , withRecord
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
  avroSchema () = plainSchema nullSchema
  toAvro () = AvroPrimitive AvroNull

instance ToAvro Bool where
  avroSchema _ = plainSchema boolSchema
  toAvro = AvroPrimitive . AvroBool

instance ToAvro Int32 where
  avroSchema _ = plainSchema intSchema
  toAvro = AvroPrimitive . AvroInt

instance ToAvro Int64 where
  avroSchema _ = plainSchema longSchema
  toAvro = AvroPrimitive . AvroLong

instance ToAvro Int where
  avroSchema _ = plainSchema longSchema
  toAvro = AvroPrimitive . AvroLong . fromIntegral

instance ToAvro Float where
  avroSchema _ = plainSchema floatSchema
  toAvro = AvroPrimitive . AvroFloat

instance ToAvro Double where
  avroSchema _ = plainSchema doubleSchema
  toAvro = AvroPrimitive . AvroDouble

instance ToAvro BS.ByteString where
  avroSchema _ = plainSchema bytesSchema
  toAvro = AvroPrimitive . AvroBytes . LB.fromStrict

instance ToAvro LB.ByteString where
  avroSchema _ = plainSchema bytesSchema
  toAvro = AvroPrimitive . AvroBytes

instance ToAvro [Word8] where
  avroSchema _ = plainSchema bytesSchema
  toAvro = AvroPrimitive . AvroBytes . LB.pack

instance ToAvro T.Text where
  avroSchema _ = plainSchema stringSchema
  toAvro = AvroPrimitive . AvroString . LT.fromStrict

instance ToAvro LT.Text where
  avroSchema _ = plainSchema stringSchema
  toAvro = AvroPrimitive . AvroString

instance ToAvro String where
  avroSchema _ = plainSchema stringSchema
  toAvro = AvroPrimitive . AvroString . LT.pack

instance ToAvro PrimitiveType where
  avroSchema AvroNull = plainSchema nullSchema
  avroSchema (AvroBool _) = plainSchema boolSchema
  avroSchema (AvroInt _) = plainSchema intSchema
  avroSchema (AvroLong _) = plainSchema longSchema
  avroSchema (AvroFloat _) = plainSchema floatSchema
  avroSchema (AvroDouble _) = plainSchema doubleSchema
  avroSchema (AvroBytes _) = plainSchema bytesSchema
  avroSchema (AvroString _) = plainSchema stringSchema
  toAvro = AvroPrimitive

instance ToAvro ComplexType where
  avroSchema (AvroNamed nm ns r) = plainSchema $ case r of
    AvroRecord f -> recordSchema nm ns [] $ map (\(fn, ft) -> recordField fn (avroSchema ft)) f
    AvroEnum _ f -> enumSchema nm ns [] f
    AvroFixed f -> fixedSchema nm ns [] (BS.length f)
  avroSchema (AvroArray f) = plainSchema . ComplexSchema $ ArraySchema (avroSchema . V.head . head $ f)
  avroSchema (AvroMap f) = plainSchema . ComplexSchema $ MapSchema (avroSchema . snd . V.head . head $ f)
  avroSchema (AvroUnion _ _ f) = plainSchema . ComplexSchema $ UnionSchema f
  toAvro = AvroComplex

instance ToAvro AvroType where
  avroSchema (AvroPrimitive f) = avroSchema f
  avroSchema (AvroComplex f) = avroSchema f
  toAvro = id

class FromAvro a where
  fromAvro :: Monad m => AvroType -> m a
  fromBinary :: Schema -> Get a
  fromBinary s = getAvro (toTypeSchema s) >>= fromAvro

instance FromAvro () where
  fromAvro (AvroPrimitive AvroNull) = return ()
  fromAvro r = fail $ "expected () got " ++ show r

instance FromAvro Bool where
  fromAvro (AvroPrimitive (AvroBool v)) = return v
  fromAvro r = fail $ "expected bool got " ++ show r

instance FromAvro Int32 where
  fromAvro (AvroPrimitive (AvroInt v)) = return v
  fromAvro r = fail $ "expected int got " ++ show r

instance FromAvro Int64 where
  fromAvro (AvroPrimitive (AvroLong v)) = return v
  fromAvro r = fail $ "expected long got " ++ show r

instance FromAvro Int where
  fromAvro (AvroPrimitive (AvroLong v)) = return (fromIntegral v)
  fromAvro (AvroPrimitive (AvroInt v)) = return (fromIntegral v)
  fromAvro r = fail $ "expected int/long got " ++ show r

instance FromAvro Float where
  fromAvro (AvroPrimitive (AvroFloat v)) = return v
  fromAvro r = fail $ "expected float got " ++ show r

instance FromAvro Double where
  fromAvro (AvroPrimitive (AvroDouble v)) = return v
  fromAvro r = fail $ "expected double got " ++ show r

instance FromAvro BS.ByteString where
  fromAvro (AvroComplex (AvroNamed _ _ (AvroFixed v))) = return v
  fromAvro (AvroPrimitive (AvroBytes v)) = return (LB.toStrict v)
  fromAvro r = fail $ "expected bytes got " ++ show r

instance FromAvro LB.ByteString where
  fromAvro (AvroComplex (AvroNamed _ _ (AvroFixed v))) = return (LB.fromStrict v)
  fromAvro (AvroPrimitive (AvroBytes v)) = return v
  fromAvro r = fail $ "expected bytes got " ++ show r

instance FromAvro [Word8] where
  fromAvro = fmap BS.unpack . fromAvro

instance FromAvro T.Text where
  fromAvro (AvroPrimitive (AvroString v)) = return (LT.toStrict v)
  fromAvro r = fail $ "expected string got " ++ show r

instance FromAvro LT.Text where
  fromAvro (AvroPrimitive (AvroString v)) = return v
  fromAvro r = fail $ "expected string got " ++ show r

instance FromAvro String where
  fromAvro = fmap T.unpack . fromAvro

instance FromAvro PrimitiveType where
  fromAvro (AvroPrimitive v) = return v
  fromAvro r = fail $ "expecting primitive got " ++ show r

instance FromAvro ComplexType where
  fromAvro (AvroComplex v) = return v
  fromAvro r = fail $ "expecting complex got " ++ show r

instance FromAvro AvroType where
  fromAvro = return

instance ToAvro a => ToAvro (Map.Map T.Text a) where
  avroSchema _ = avroSchema $ AvroMap (undefined :: [V.Vector a])
  toAvro = toAvro . AvroMap . return . V.fromList . map (second toAvro) . Map.toList

instance FromAvro a => FromAvro (Map.Map T.Text a) where
  fromAvro (AvroComplex (AvroMap r)) =
    fmap Map.fromList . mapM go . concatMap V.toList $ r
    where go (x, y) = (x,) <$> fromAvro y
  fromAvro _ = fail "only avro map can be a haskell map"

flookup :: (FromAvro r, Monad m) => String -> [(String, AvroType)] -> m r
flookup s = maybe (fail $ "no field " ++ s) fromAvro . lookup s

withRecord :: Monad m => ([(String, AvroType)] -> m r) -> AvroType -> m r
withRecord f (AvroComplex (AvroNamed _ _ (AvroRecord r))) = f r
withRecord _ _ = fail "could not match record type"
