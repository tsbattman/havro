{-# LANGUAGE OverloadedStrings #-}

module Main where

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

main :: IO ()
main = do
  l <- LB.readFile "/home/data/capture/suntime/CON_FORECAST.avc"
  let avc = parseContainer l
  print . dataCodec . containerHeader $ avc
  print . dataSchema . containerHeader $ avc
  print . encode . dataSchema . containerHeader $ avc
  mapM_ (print . blockSync) . containerBlocks $ avc
