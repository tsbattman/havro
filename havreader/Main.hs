{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Maybe (fromMaybe)
import Text.Printf

import Data.Aeson
import Data.Binary.Get
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LB
import qualified Data.Map as Map
import qualified Data.Vector as V

import Data.Avro.Container
import Data.Avro.Generic
import Data.Avro.Schema
import Data.Avro.Class

printGeneric :: Schema -> LB.ByteString -> IO ()
printGeneric schema = mapM_ (print :: AvroType -> IO ()) . take 5 . parseData schema

main :: IO ()
main = do
  l <- LB.readFile "/home/data/capture/suntime/CON_FORECAST.avc"
  let avc = parseContainer l
      header = containerHeader avc
      codecName = dataCodec header
      schema = fromMaybe (error "no schema") . dataSchema $ header
  print codecName
  print schema
  case lookupCodec header predefinedCodecs of
    Nothing -> putStrLn $ printf "no coded %s" (show codecName)
    Just codec -> mapM_ (printGeneric schema . decompressUsing codec . blockData)  . containerBlocks $ avc
