# -----------------------------------------------------------------------------------
# �t�@�C�����F	�قȂ�DB�Ԃ̃f�[�^�A�g����
# �@�\�����F	DbSampleA����擾�����f�[�^��DbSampleB�ɏ�������
# 				DbSampleB�ɏ������񂾃f�[�^�����Ɏ擾�����f�[�^��DbSampleA�ɏ�������
# 				DbSampleA�ŃX�g�A�h�v���V�[�W���ɂ�鏈�������s
# -----------------------------------------------------------------------------------

# ���C�u�����ǂݍ���
library(DBI);
library(odbc);

# �֐���`
# -----------------------------------------------------------------------------------
# �֐����F	WriteTableB�iDbSampleB�̃e�[�u���������ݏ����j
# ����1�F	DbConB�iDB�ڑ��I�u�W�F�N�g�j
# ����2�F	DfId�i�Ώ�ID�̃f�[�^�t���[���j
# �߂�l�F	����=NULL / �ُ�=�G���[���b�Z�[�W
# -----------------------------------------------------------------------------------
WriteTableB <- function(DbConB, DfId) {
  tryCatch(
  	expr = {
  		# �g�����U�N�V�����J�n
  		DbTranB <- dbBegin(DbConB);
  
  		# �e�[�u����������
		dbExecute(DbConB, 'DELETE FROM "TestTableB1";');
		
		# �擾�����f�[�^��TestTableB1�ɏ�������
		if (dbWriteTable(DbConB, 'TestTableB1', DfId, overwrite=TRUE) != TRUE) {
			ErrMsg <- 'TestTableB1�̏������݂Ɏ��s���܂���';
			stop(ErrMsg);
		}
  
  		# �R�~�b�g
		if (dbCommit(DbConB) != TRUE) {
			ErrMsg <- '�R�~�b�g�Ɏ��s���܂���[B]';
			stop(ErrMsg);
		}
  
  		return(NULL);
  	},
  	error = function(e) {
  		# ���O���b�Z�[�W�쐬
  		LogMsg <- paste0('�y', Sys.time(), '�z ', '�G���[�������������߁A�������m�F���ĉ������@�ˁ@', conditionMessage(e));
  
  		# ���[���o�b�N
		if (exists('DbTranB') == TRUE) {dbRollback(DbConB);}
  
  		return(LogMsg);
	}
  )
}

# -----------------------------------------------------------------------------------
# �֐����F	WriteTableA�iDbSampleA�̃e�[�u���������ݏ����j
# ����1�F	DbConA�iDB�ڑ��I�u�W�F�N�g�j
# ����2�F	DfLogB2�i���OB2�̃f�[�^�t���[���j
# ����3�F	DfLogB3�i���OB3�̃f�[�^�t���[���j
# �߂�l�F	����=NULL / �ُ�=�G���[���b�Z�[�W
# -----------------------------------------------------------------------------------
WriteTableA <- function(DbConA, DfLogB2, DfLogB3) {
  tryCatch(
	expr = {
		# �g�����U�N�V�����J�n
		DbTranA <- dbBegin(DbConA);

		# �e�[�u����������
  		dbExecute(MartCon, 'TRUNCATE TABLE TestTableA2;');
  		dbExecute(MartCon, 'TRUNCATE TABLE TestTableA3;');

		# TestTableA2�ɏ�������
		if (dbWriteTable(DbConA, 'TestTableA2', DfLogB2, overwrite=TRUE) != TRUE) {
			ErrMsg <- 'TestTableA2�̏������݂Ɏ��s���܂���';
			stop(ErrMsg);
		}
		# TestTableA3�ɏ�������
		if (dbWriteTable(DbConA, 'TestTableA3', DfLogB3, overwrite=TRUE) != TRUE) {
			ErrMsg <- 'TestTableA3�̏������݂Ɏ��s���܂���';
			stop(ErrMsg);
		}

		# �R�~�b�g
		if (dbCommit(DbConA) != TRUE) {
			ErrMsg <- '�R�~�b�g�Ɏ��s���܂���[A]';
			stop(ErrMsg);
		}
  
  		return(NULL);
  	},
  	error = function(e) {
  		# ���O�o��
  		LogMsg <- paste0('�y', Sys.time(), '�z ', '�G���[�������������߁A�������m�F���ĉ������@�ˁ@', conditionMessage(e));
  
  		# ���[���o�b�N
		if (exists('DbTranA') == TRUE) {dbRollback(DbConA);}
  
  		return(LogMsg);
  	}
  )
}

# -----------------------------------------------------------------------------------
# �֐����F	�Ȃ��i�又���j
# �����F	�Ȃ�
# �߂�l�F	�Ȃ�
# -----------------------------------------------------------------------------------
tryCatch(
	expr = {
		# ���O�t�@�C�����쐬
		LogPath <- 'C:\\Work\\Log\\';
		LogFileName <- paste0('Prefix_Log_', format(Sys.Date(), '%Y%m%d'), '.log');
		LogFile <- file(paste0(LogPath, LogFileName), open = 'a');
		# ���O�o��
		LogMsg <- paste0('�y', Sys.time(), '�z ', '�������J�n���܂�');
		writeLines(LogMsg, LogFile);

		# DB�ڑ�(�f�[�^�x�[�XA��ODBC�ڑ�)
		DbConA <- dbConnect(
			drv = odbc(),
			dsn = 'DbSampleA',
			uid = 'abc',
			pwd	= 'xyz',
			encoding = 'shift_jis'
		);

		# �f�[�^�x�[�XA����f�[�^���擾
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

		# DB�ڑ�(�f�[�^�x�[�XB��ODBC�ڑ�)
		DbConB <- dbConnect(
			drv = odbc(),
			dsn = 'DbSampleB'
		);

		# ���O�o��
		LogMsg <- paste0('�y', Sys.time(), '�z ', 'DbSampleB�Ƀf�[�^����������');
		writeLines(LogMsg, LogFile);

		# DbSampleA����擾�����f�[�^��DbSampleB�ɏ�������
		ResultMsg <- WriteTableB(DbConB, DfId);
		if (is.null(ResultMsg) != TRUE) {stop(ResultMsg);}

		# TestTableB2����f�[�^���擾
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

		# TestTableB3����f�[�^���擾
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

		# DbSampleB�̐ڑ���ؒf
		dbDisconnect(DbConB);

		# ���O�o��
		LogMsg <- paste0('�y', Sys.time(), '�z ', 'DbSampleA�Ƀf�[�^����������');
		writeLines(LogMsg, LogFile);

		# DbSampleB����擾�����f�[�^��DbSampleA�ɏ�������
		ResultMsg <- WriteTableA(DbConA, DfLogB2, DfLogB3);
		if (is.null(ResultMsg) != TRUE) {stop(ResultMsg);}

		# ���O�o��
		LogMsg <- paste0('�y', Sys.time(), '�z ', '�X�g�A�h�v���V�[�W�������s');
		writeLines(LogMsg, LogFile);

		# DbSampleA�̃X�g�A�h�v���V�[�W�������s���āA���ʃZ�b�g���󂯎��
		DfResult <- dbGetQuery(DbConA, 'EXECUTE [SampleProcedure];');
		if (DfResult[1, 1] == -1) {
			DbErrCode <- DfResult[1, 2];
			DbErrMsg <- DfResult[1, 3]
			ErrMsg <- paste0('[', DbErrCode, ']', DbErrMsg);
			stop(ErrMsg);
		}

		# ���O�o��
		LogMsg <- paste0('�y', Sys.time(), '�z ', '�������I�����܂�');
		writeLines(LogMsg, LogFile);
	},
	error = function(e) {
		# ���O�o��
		LogMsg <- paste0('�y', Sys.time(), '�z ', '�G���[�������������߁A�������m�F���ĉ������@�ˁ@', conditionMessage(e));
		writeLines(LogMsg, LogFile);
	},
	finally = {
		# DB�ڑ���ؒf
		if ((exists('DbConA') == TRUE) && (dbIsValid(DbConA) == TRUE)) {dbDisconnect(DbConA);}
		if ((exists('DbConB') == TRUE) && (dbIsValid(DbConB) == TRUE)) {dbDisconnect(DbConB);}

		# ���O�t�@�C�������
		if (exists('LogFile') == TRUE) {close(LogFile);}
	}
)
