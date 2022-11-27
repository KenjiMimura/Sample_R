# -----------------------------------------------------------------------------------
# ファイル名：	異なるDB間のデータ連携処理
# 機能説明：	DbSampleAから取得したデータをDbSampleBに書き込み
# 				DbSampleBに書き込んだデータを元に取得したデータをDbSampleAに書き込み
# 				DbSampleAでストアドプロシージャによる処理を実行
# -----------------------------------------------------------------------------------

# ライブラリ読み込み
library(DBI);
library(odbc);

# 関数定義
# -----------------------------------------------------------------------------------
# 関数名：	WriteTableB（DbSampleBのテーブル書き込み処理）
# 引数1：	DbConB（DB接続オブジェクト）
# 引数2：	DfId（対象IDのデータフレーム）
# 戻り値：	正常=NULL / 異常=エラーメッセージ
# -----------------------------------------------------------------------------------
WriteTableB <- function(DbConB, DfId) {
  tryCatch(
  	expr = {
  		# トランザクション開始
  		DbTranB <- dbBegin(DbConB);
  
  		# テーブルを初期化
		dbExecute(DbConB, 'DELETE FROM "TestTableB1";');
		
		# 取得したデータをTestTableB1に書き込み
		if (dbWriteTable(DbConB, 'TestTableB1', DfId, overwrite=TRUE) != TRUE) {
			ErrMsg <- 'TestTableB1の書き込みに失敗しました';
			stop(ErrMsg);
		}
  
  		# コミット
		if (dbCommit(DbConB) != TRUE) {
			ErrMsg <- 'コミットに失敗しました[B]';
			stop(ErrMsg);
		}
  
  		return(NULL);
  	},
  	error = function(e) {
  		# ログメッセージ作成
  		LogMsg <- paste0('【', Sys.time(), '】 ', 'エラーが発生したため、処理を確認して下さい　⇒　', conditionMessage(e));
  
  		# ロールバック
		if (exists('DbTranB') == TRUE) {dbRollback(DbConB);}
  
  		return(LogMsg);
	}
  )
}

# -----------------------------------------------------------------------------------
# 関数名：	WriteTableA（DbSampleAのテーブル書き込み処理）
# 引数1：	DbConA（DB接続オブジェクト）
# 引数2：	DfLogB2（ログB2のデータフレーム）
# 引数3：	DfLogB3（ログB3のデータフレーム）
# 戻り値：	正常=NULL / 異常=エラーメッセージ
# -----------------------------------------------------------------------------------
WriteTableA <- function(DbConA, DfLogB2, DfLogB3) {
  tryCatch(
	expr = {
		# トランザクション開始
		DbTranA <- dbBegin(DbConA);

		# テーブルを初期化
  		dbExecute(MartCon, 'TRUNCATE TABLE TestTableA2;');
  		dbExecute(MartCon, 'TRUNCATE TABLE TestTableA3;');

		# TestTableA2に書き込み
		if (dbWriteTable(DbConA, 'TestTableA2', DfLogB2, overwrite=TRUE) != TRUE) {
			ErrMsg <- 'TestTableA2の書き込みに失敗しました';
			stop(ErrMsg);
		}
		# TestTableA3に書き込み
		if (dbWriteTable(DbConA, 'TestTableA3', DfLogB3, overwrite=TRUE) != TRUE) {
			ErrMsg <- 'TestTableA3の書き込みに失敗しました';
			stop(ErrMsg);
		}

		# コミット
		if (dbCommit(DbConA) != TRUE) {
			ErrMsg <- 'コミットに失敗しました[A]';
			stop(ErrMsg);
		}
  
  		return(NULL);
  	},
  	error = function(e) {
  		# ログ出力
  		LogMsg <- paste0('【', Sys.time(), '】 ', 'エラーが発生したため、処理を確認して下さい　⇒　', conditionMessage(e));
  
  		# ロールバック
		if (exists('DbTranA') == TRUE) {dbRollback(DbConA);}
  
  		return(LogMsg);
  	}
  )
}

# -----------------------------------------------------------------------------------
# 関数名：	なし（主処理）
# 引数：	なし
# 戻り値：	なし
# -----------------------------------------------------------------------------------
tryCatch(
	expr = {
		# ログファイルを作成
		LogPath <- 'C:\\Work\\Log\\';
		LogFileName <- paste0('Prefix_Log_', format(Sys.Date(), '%Y%m%d'), '.log');
		LogFile <- file(paste0(LogPath, LogFileName), open = 'a');
		# ログ出力
		LogMsg <- paste0('【', Sys.time(), '】 ', '処理を開始します');
		writeLines(LogMsg, LogFile);

		# DB接続(データベースAにODBC接続)
		DbConA <- dbConnect(
			drv = odbc(),
			dsn = 'DbSampleA',
			uid = 'abc',
			pwd	= 'xyz',
			encoding = 'shift_jis'
		);

		# データベースAからデータを取得
		DfId <- dbGetQuery(
			DbConA,
			'
			SELECT DISTINCT
				[test_id]
			FROM
				[TestTableA1]
			ORDER BY
				[test_id]
			;'
		);

		# DB接続(データベースBにODBC接続)
		DbConB <- dbConnect(
			drv = odbc(),
			dsn = 'DbSampleB'
		);

		# ログ出力
		LogMsg <- paste0('【', Sys.time(), '】 ', 'DbSampleBにデータを書き込み');
		writeLines(LogMsg, LogFile);

		# DbSampleAから取得したデータをDbSampleBに書き込み
		ResultMsg <- WriteTableB(DbConB, DfId);
		if (is.null(ResultMsg) != TRUE) {stop(ResultMsg);}

		# TestTableB2からデータを取得
		DfLogB2 <- dbGetQuery(
			DbConB,
			'
			SELECT
				b2.*
			FROM
			"TestTableB2" b2
			INNER JOIN "TestTableB1" b1
				ON b1."test_id" = b2."test_id"
			;'
		);

		# TestTableB3からデータを取得
		DfLogB3 <- dbGetQuery(
			DbConB,
			'
			SELECT
				b3.*
			FROM
			"TestTableB3" b3
			INNER JOIN "TestTableB1" b1
				ON b1."test_id" = b3."test_id"
			;'
		);

		# DbSampleBの接続を切断
		dbDisconnect(DbConB);

		# ログ出力
		LogMsg <- paste0('【', Sys.time(), '】 ', 'DbSampleAにデータを書き込み');
		writeLines(LogMsg, LogFile);

		# DbSampleBから取得したデータをDbSampleAに書き込み
		ResultMsg <- WriteTableA(DbConA, DfLogB2, DfLogB3);
		if (is.null(ResultMsg) != TRUE) {stop(ResultMsg);}

		# ログ出力
		LogMsg <- paste0('【', Sys.time(), '】 ', 'ストアドプロシージャを実行');
		writeLines(LogMsg, LogFile);

		# DbSampleAのストアドプロシージャを実行して、結果セットを受け取る
		DfResult <- dbGetQuery(DbConA, 'EXECUTE [SampleProcedure];');
		if (DfResult[1, 1] == -1) {
			DbErrCode <- DfResult[1, 2];
			DbErrMsg <- DfResult[1, 3]
			ErrMsg <- paste0('[', DbErrCode, ']', DbErrMsg);
			stop(ErrMsg);
		}

		# ログ出力
		LogMsg <- paste0('【', Sys.time(), '】 ', '処理を終了します');
		writeLines(LogMsg, LogFile);
	},
	error = function(e) {
		# ログ出力
		LogMsg <- paste0('【', Sys.time(), '】 ', 'エラーが発生したため、処理を確認して下さい　⇒　', conditionMessage(e));
		writeLines(LogMsg, LogFile);
	},
	finally = {
		# DB接続を切断
		if ((exists('DbConA') == TRUE) && (dbIsValid(DbConA) == TRUE)) {dbDisconnect(DbConA);}
		if ((exists('DbConB') == TRUE) && (dbIsValid(DbConB) == TRUE)) {dbDisconnect(DbConB);}

		# ログファイルを閉じる
		if (exists('LogFile') == TRUE) {close(LogFile);}
	}
)
