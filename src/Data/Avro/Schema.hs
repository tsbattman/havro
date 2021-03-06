{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Data.Avro.Schema (
    PrimitiveSchemaType(..)
  , SortOrder(..)
  , RecordField(..)
  , recordField
  , NamedSchemaType(..)
  , ComplexSchemaType(..)
  , nullSchema
  , boolSchema
  , intSchema
  , longSchema
  , floatSchema
  , doubleSchema
  , bytesSchema
  , stringSchema
  , recordSchema
  , enumSchema
  , fixedSchema
  , arraySchema
  , mapSchema
  , unionSchema
  , TypeSchema(..)
  , Schema(..)
  , plainSchema
  , toTypeSchema
  ) where

import Control.Applicative
import Data.Monoid
import Data.Maybe (fromMaybe)

import Data.Aeson
import Data.Aeson.Types
import qualified Data.HashMap.Lazy as HashMap

data PrimitiveSchemaType =
    NullSchema | BoolSchema | IntSchema
  | LongSchema | FloatSchema | DoubleSchema
  | BytesSchema | StringSchema
  deriving (Eq, Show, Read)

instance ToJSON PrimitiveSchemaType where
  toJSON NullSchema = "null"
  toJSON BoolSchema = "bool"
  toJSON IntSchema = "int"
  toJSON LongSchema = "long"
  toJSON FloatSchema = "float"
  toJSON DoubleSchema = "double"
  toJSON BytesSchema = "bytes"
  toJSON StringSchema = "string"

instance FromJSON PrimitiveSchemaType where
  parseJSON (String "null") = pure NullSchema
  parseJSON (String "bool") = pure BoolSchema
  parseJSON (String "int") = pure IntSchema
  parseJSON (String "long") = pure LongSchema
  parseJSON (String "float") = pure FloatSchema
  parseJSON (String "double") = pure DoubleSchema
  parseJSON (String "bytes") = pure BytesSchema
  parseJSON (String "string") = pure StringSchema
  parseJSON v = typeMismatch "expected primitive type" v

data SortOrder = Ascending | Descending | Ignore
  deriving (Eq, Show, Read)

instance ToJSON SortOrder where
  toJSON Ascending = "ascending"
  toJSON Descending = "descending"
  toJSON Ignore = "ignore"

instance FromJSON SortOrder where
  parseJSON (String "ascending") = pure Ascending
  parseJSON (String "descending") = pure Descending
  parseJSON (String "ignore") = pure Ignore
  parseJSON v = typeMismatch "expected sort order" v

data RecordField = RecordField {
    fieldName :: String
  , fieldDoc :: String
  , fieldType :: Schema
  , fieldDefault :: Maybe () -- TODO: replace () with AvroType, need to break circular dependency
  , fieldOrder :: SortOrder
  , fieldAliases :: [String]
  } deriving (Eq, Show, Read)

recordField :: String -> Schema -> RecordField
recordField nm s = RecordField nm "" s Nothing Ascending []

instance ToJSON RecordField where
  toJSON RecordField{..} = object $ [
      "name" .= fieldName
    , "type" .= fieldType
    , "order" .= fieldOrder
    ]
    <> if null fieldDoc then [] else ["doc" .= fieldDoc]
    <> if null fieldAliases then [] else ["aliases" .= fieldAliases]

instance FromJSON RecordField where
  parseJSON = withObject "field" $ \o ->
    RecordField <$>
          o .: "name"
      <*> fmap (fromMaybe "") (o .:? "doc")
      <*> o .: "type"
      <*> pure Nothing
      <*> fmap (fromMaybe Ascending) (o .:? "order")
      <*> fmap (fromMaybe []) (o .:? "aliases")

data NamedSchemaType =
    RecordSchema [RecordField]
  | EnumSchema [String]
  | FixedSchema Int
  deriving (Eq, Show, Read)

instance ToJSON NamedSchemaType where
  toJSON (RecordSchema r) = object ["type" .= String "record", "fields" .= r]
  toJSON (EnumSchema r) = object ["type" .= String "enum", "symbols" .= r]
  toJSON (FixedSchema r) = object ["type" .= String "fixed", "size" .= r]

instance FromJSON NamedSchemaType where
  parseJSON = withObject "named" $ \o -> do
    v <- o .: "type"
    case v of
      String "record" -> RecordSchema <$> o .: "fields"
      String "enum" -> EnumSchema <$> o .: "symbols"
      String "fixed" -> FixedSchema <$> o .: "size"
      _ -> typeMismatch "named schema" v

data ComplexSchemaType =
    NamedSchema String (Maybe String) [String] NamedSchemaType
  | ArraySchema Schema
  | MapSchema Schema
  | UnionSchema [Schema]
  deriving (Eq, Show, Read)

instance ToJSON ComplexSchemaType where
  toJSON (NamedSchema nm ns alias s) = case toJSON s of
    Object o -> Object $ o <> a
    _ -> error "should never happen: NamedSchemaType should always return an object"
    where
        a = HashMap.fromList $ ["name" .= nm]
          <> if null alias then [] else ["aliases" .= alias]
          <> maybe [] (\n -> ["namespace" .= n]) ns
  toJSON (ArraySchema s) = object ["name" .= String "array", "items" .= s]
  toJSON (MapSchema s) = object ["name" .= String "map", "values" .= s]
  toJSON (UnionSchema s) = toJSON s

instance FromJSON ComplexSchemaType where
  parseJSON (Object o) = do
    v <- o .: "name"
    case v of
      String "array" -> ArraySchema <$> o .: "items"
      String "map" -> MapSchema <$>  o .: "values"
      _ -> NamedSchema <$> o .: "name"
        <*> o .:? "doc"
        <*> fmap (fromMaybe []) (o .:? "aliases")
        <*> parseJSON (Object o)
  parseJSON (Array a) = UnionSchema <$> parseJSON (Array a)
  parseJSON v = typeMismatch "complex schema" v

data TypeSchema =
    PrimitiveSchema PrimitiveSchemaType
  | ComplexSchema ComplexSchemaType
  deriving (Eq, Show, Read)

nullSchema, boolSchema, intSchema, longSchema, floatSchema, doubleSchema, bytesSchema, stringSchema :: TypeSchema
nullSchema = PrimitiveSchema NullSchema
boolSchema = PrimitiveSchema BoolSchema
intSchema = PrimitiveSchema IntSchema
longSchema = PrimitiveSchema LongSchema
floatSchema = PrimitiveSchema FloatSchema
doubleSchema = PrimitiveSchema DoubleSchema
bytesSchema = PrimitiveSchema BytesSchema
stringSchema = PrimitiveSchema StringSchema

recordSchema :: String -> Maybe String -> [String] -> [RecordField] -> TypeSchema
recordSchema nm ns alias f = ComplexSchema $ NamedSchema nm ns alias (RecordSchema f)

enumSchema :: String -> Maybe String -> [String] -> [String] -> TypeSchema
enumSchema nm ns alias f = ComplexSchema $ NamedSchema nm ns alias (EnumSchema f)

fixedSchema :: String -> Maybe String -> [String] -> Int -> TypeSchema
fixedSchema nm ns alias n = ComplexSchema $ NamedSchema nm ns alias (FixedSchema n)

arraySchema, mapSchema :: Schema -> TypeSchema
arraySchema = ComplexSchema . ArraySchema
mapSchema = ComplexSchema .  MapSchema

unionSchema :: [Schema] -> TypeSchema
unionSchema = ComplexSchema . UnionSchema

instance ToJSON TypeSchema where
  toJSON (PrimitiveSchema s) = toJSON s
  toJSON (ComplexSchema s) = toJSON s

instance FromJSON TypeSchema where
  parseJSON v@(String _) = PrimitiveSchema <$> parseJSON v
  parseJSON v@(Array _) = ComplexSchema <$> parseJSON v
  parseJSON v@(Object o) =
        (PrimitiveSchema <$> o .: "type")
    <|> (ComplexSchema <$> parseJSON v)
  parseJSON v = typeMismatch "type schema" v

data Schema = WithAttributes TypeSchema Object
  deriving (Eq, Show, Read)

instance ToJSON Schema where
  toJSON (WithAttributes s a)
    | HashMap.null a = toJSON s
    | otherwise = case toJSON s of
        Object o -> Object $ o <> a
        String t -> Object $ HashMap.fromList ["type" .= t] <> a
        _ -> error "should not happen: only string or object supported for schema"

instance FromJSON Schema where
  parseJSON v@(Array _) = WithAttributes <$> fmap ComplexSchema (parseJSON v) <*> pure HashMap.empty
  parseJSON v@(String _) = WithAttributes <$> fmap PrimitiveSchema (parseJSON v) <*> pure HashMap.empty
  parseJSON v@(Object o) = do
    r <- parseJSON v
    return $ case toJSON r of
      Object ro -> WithAttributes r (HashMap.difference o ro)
      String _ -> WithAttributes r (HashMap.delete "type" o)
      _ -> error "schema can be string or object"
  parseJSON v = typeMismatch "top level schema can be string, object, or array" v

plainSchema :: TypeSchema -> Schema
plainSchema = (`WithAttributes` HashMap.empty)

toTypeSchema :: Schema -> TypeSchema
toTypeSchema (WithAttributes s _) = s
